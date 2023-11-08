from typing import Union, List

from fastapi import FastAPI, HTTPException

from rdp.sensor import Reader
from rdp.crud import create_engine, Crud
from . import api_types as ApiTypes
import logging

logger = logging.getLogger("rdp.api")
app = FastAPI()

@app.get("/")
def read_root() -> ApiTypes.ApiDescription:
    """This url returns a simple description of the api

    Returns:
        ApiTypes.ApiDescription: the Api description in json format 
    """    
    return ApiTypes.ApiDescription()

@app.get("/type/")
def read_types() -> List[ApiTypes.ValueType]:
    """Implements the get of all value types

    Returns:
        List[ApiTypes.ValueType]: list of available valuetypes. 
    """    
    global crud
    return crud.get_value_types()

@app.get("/type/{id}/")
def read_type(id: int) -> ApiTypes.ValueType:
    """returns an explicit value type identified by id

    Args:
        id (int): primary key of the desired value type

    Raises:
        HTTPException: Thrown it a value type with the given id cannot be accessed

    Returns:
        ApiTypes.ValueType: the desired value type 
    """
    global crud
    try:
         return crud.get_value_type(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found") 
    return value_type 

@app.put("/type/{id}/")
def put_type(id, value_type: ApiTypes.ValueTypeNoID) -> ApiTypes.ValueType:
    """PUT request to a specail valuetype. This api call is used to change a value type object.

    Args:
        id (int): primary key of the requested value type
        value_type (ApiTypes.ValueTypeNoID): json object representing the new state of the value type. 

    Raises:
        HTTPException: Thrown if a value type with the given id cannot be accessed 

    Returns:
        ApiTypes.ValueType: the requested value type after persisted in the database. 
    """
    global crud
    try:
        crud.add_or_update_value_type(id, value_type_name=value_type.type_name, value_type_unit=value_type.type_unit)
        return read_type(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found")

@app.get("/value/")
def get_values(type_id:int=None, start:int=None, end:int=None) -> List[ApiTypes.Value]:
    """Get values from the database. The default is to return all available values. This result can be filtered.

    Args:
        type_id (int, optional): If set, only values of this type are returned. Defaults to None.
        start (int, optional): If set, only values at least as new are returned. Defaults to None.
        end (int, optional): If set, only values not newer than this are returned. Defaults to None.

    Raises:
        HTTPException: _description_

    Returns:
        List[ApiTypes.Value]: _description_
    """
    global crud
    try:
        values = crud.get_values(type_id, start, end)
        return values
    except crud.NoResultFound:
        raise HTTPException(status_code=404, deltail="Item not found")

@app.on_event("startup")
async def startup_event() -> None:
    """start the character device reader
    """    
    logger.info("STARTUP: Sensor reader!")
    global reader, crud
    engine = create_engine("sqlite:///rdb.test.db")
    crud = Crud(engine)
    reader = Reader(crud)
    reader.start()
    logger.debug("STARTUP: Sensore reader completed!")

@app.on_event("shutdown")
async def startup_event():
    """stop the character device reader
    """    
    global reader
    logger.debug("SHUTDOWN: Sensor reader!")
    reader.stop()
    logger.info("SHUTDOWN: Sensor reader completed!")


@app.put("/device/{name}/")
def put_type(id, device: ApiTypes.Device) -> ApiTypes.Device:
    """PUT request to a device. This api call is used to change/add a device.

    Args:
        id (int): primary key of the requested value type
        value_type (ApiTypes.ValueTypeNoID): json object representing the new state of the value type. 

    Raises:
        HTTPException: Thrown if a value type with the given id cannot be accessed 

    Returns:
        ApiTypes.ValueType: the requested value type after persisted in the database. 
    """
    global crud
    try:
        crud.add_or_update_device(id, device_name=device.name, device_location=device.location)
        return read_device(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Item not found")



@app.get("/device/{id}/")
def read_device(id: int) -> ApiTypes.Device:
    """Returns a device type identified by id

    Args:
        id (int): Primary key of the desired device type

    Raises:
        HTTPException: Thrown if a device type with the given id cannot be accessed

    Returns:
        ApiTypes.DeviceType: The desired device type
    """
    global crud
    try:
        return crud.get_device_type(id)
    except crud.NoResultFound:
        raise HTTPException(status_code=404, detail="Device not found")


@app.post("/value/")
def post_value(value: ApiTypes.ValueNoID):
    crud.add_value(value_time=value.time, value_type=value.value_type_id, value_value=value.value)
    return "Value added!"
