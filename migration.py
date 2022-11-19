import asyncio
from progress.bar import IncrementalBar
from more_itertools import chunked
from aiohttp import ClientSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

BASE_URL = 'https://swapi.dev/api/people/'
CHUNK_SIZE = 10
PG_DSN = 'postgresql+asyncpg://app:1234@127.0.0.1:5431/data'

engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

bar = IncrementalBar('Loading characters into the database', max=83)
bar.start()


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    height = Column(String)
    mass = Column(String)
    hair_color = Column(String)
    skin_color = Column(String)
    eye_color = Column(String)
    birth_year = Column(String)
    gender = Column(String)
    homeworld = Column(String)
    films = Column(String)
    species = Column(String)
    vehicles = Column(String)
    starships = Column(String)


async def chunked_async(async_iter, size):

    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            yield buffer
            break
        buffer.append(item)
        bar.next()
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_filds(url_list, session: ClientSession, fild="name"):
    filds = []
    for url in url_list:
        async with session.get(url) as response:
            data = await response.json()
            filds.append(data[fild])
    return ', '.join(filds)


async def get_person(person_id: int, session: ClientSession):
    async with session.get(f'{BASE_URL}{person_id}/') as response:
        if response.status == 404:
            return
        json_data = await response.json()
        pesron = {
            "id": person_id,
            "birth_year": json_data["birth_year"],
            "eye_color": json_data["eye_color"],
            "films": await get_filds(json_data["films"], session, fild="title"),
            "gender": json_data["gender"],
            "hair_color": json_data["hair_color"],
            "height": json_data["height"],
            "homeworld": await get_filds([json_data["homeworld"]], session),
            "mass": json_data["mass"],
            "name": json_data["name"],
            "skin_color": json_data["skin_color"],
            "species": await get_filds(json_data["species"], session),
            "starships": await get_filds(json_data["starships"], session),
            "vehicles": await get_filds(json_data["vehicles"], session),
        }
        return pesron


async def get_people(person_ids: list):
    async with ClientSession() as session:
        for chunk in chunked(person_ids, CHUNK_SIZE):
            person_list = [get_person(person_id, session=session) for person_id in chunk]
            persons_chunk = await asyncio.gather(*person_list)
            for item in persons_chunk:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(**item) for item in people_chunk if item is not None])
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for item in chunked_async(get_people(range(1, 84)), CHUNK_SIZE):
        asyncio.create_task(insert_people(item))

    tasks_in_work = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks_in_work:
        await task


async def tasks():
    await main()

asyncio.get_event_loop().run_until_complete(tasks())
bar.finish()
