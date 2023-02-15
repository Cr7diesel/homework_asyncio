import asyncio
import requests
from aiohttp import ClientSession
from more_itertools import chunked

from models import Session, SwapiPeople, engine, Base

CHUNK_SIZE = 10
ALL_PEOPLE = requests.get('https://swapi.dev/api/people/').json()['count']


async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_url(url, key, session):
    async with session.get(f'{url}') as response:
        data = await response.json()
        return data[key]


async def tasks_urls(urls, key, session):
    tasks = (asyncio.create_task(get_url(url, key, session)) for url in urls)
    for task in tasks:
        yield await task


async def urls_data(urls, key, session):
    result = []
    async for item in tasks_urls(urls, key, session):
        result += item
    return ', '.join(result)


async def add_to_db(people_chunk):
    async with Session() as session:
        async with ClientSession() as session_2:
            for data in people_chunk:
                if data.get('status') == 404:
                    break
                person = SwapiPeople(
                    birth_year=data['birth_year'],
                    eye_color=data['eye_color'],
                    gender=data['gender'],
                    hair_color=data['hair_color'],
                    height=data['height'],
                    mass=data['mass'],
                    name=data['name'],
                    skin_color=data['skin_color'],
                    homeworld=await urls_data([data['homeworld']], 'name', session_2),
                    films=await urls_data(data['films'], 'title', session_2),
                    species=await urls_data(data['species'], 'name', session_2),
                    starships=await urls_data(data['starships'], 'name', session_2),
                    vehicles=await urls_data(data['vehicles'], 'name', session_2),
                )
                session.add(person)
                await session.commit()


async def get_person(person_id: int, session: ClientSession):
    async with session.get(f'https://swapi.dev/api/people/{person_id}') as response:
        if response.status == 404:
            return {'status': 404}
        person = await response.json()
        return person


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, ALL_PEOPLE + 1), CHUNK_SIZE):
            coroutines = [get_person(i, session=session) for i in chunk]
            people = await asyncio.gather(*coroutines)
            for item in people:
                yield item


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(add_to_db(chunk))
    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task

if __name__ == '__main__':
    asyncio.run(main())
