import asyncio
import datetime

import aiohttp
import more_itertools

from models import SessionDB, SwapiPeople, init_orm

MAX_REQUESTS = 5


async def get_person(person_id, session):
    response = await session.get(f"https://swapi.dev/api/people/{person_id}/")
    json_data = await response.json()
    json_data.pop("created", None)
    json_data.pop("edited", None)
    json_data.pop("url", None)
    return json_data


async def insert_people(people_list):
    async with SessionDB() as session:
        try:
            orm_model_list = [SwapiPeople(**person_dict) for person_dict in people_list]
        except TypeError:
            await session.commit()
            return 1
        session.add_all(orm_model_list)
        await session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as session_http:
        coros = (get_person(i, session_http) for i in range(1, 101))
        for coros_chunk in more_itertools.chunked(coros, 5):
            people_list = await asyncio.gather(*coros_chunk)
            asyncio.create_task(insert_people(people_list))

        tasks = asyncio.all_tasks()
        main_task = asyncio.current_task()
        tasks.remove(main_task)
        await asyncio.gather(*tasks)


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
