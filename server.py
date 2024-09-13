import asyncio
import datetime

import aiohttp
import more_itertools

from models import SessionDB, SwapiPeople, init_orm

MAX_REQUESTS = 5

async def urls_to_str(person_dict: dict, row_names: list, session):
    for row_name in row_names:
        values = person_dict[row_name]
        if row_name == "homeworld":
            str_values = await get_homeland(values, session)
            person_dict[row_name] = str_values
            continue
        str_values = []
        for url in values:
            response = await session.get(url)
            json_data = await response.json()
            if row_name == "films":
                str_values.append(json_data["title"])
            else:
                str_values.append(json_data["name"])
        person_dict[row_name] = str_values
    return person_dict

async def get_homeland(value, session):
    response = await session.get(value)
    json_data = await response.json()
    return json_data["name"]

def check_if_detail(person_dict):
    if person_dict == {'detail': 'Not found'}:
        return True

async def get_person(person_id, session):
    response = await session.get(f"https://swapi.dev/api/people/{person_id}/")
    json_data_to_modify = await response.json()
    if check_if_detail(json_data_to_modify):
        return 
    json_data = await urls_to_str(json_data_to_modify, ["films", "species", "starships", "vehicles", "homeworld"], session)
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


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
