import logging
from typing import List

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session

from .model import Base, Value, ValueType, DeviceType


class Crud:
    def __init__(self, engine):
        self._engine = engine
        self.IntegrityError = IntegrityError
        self.NoResultFound = NoResultFound

        Base.metadata.create_all(self._engine)

    def add_or_update_value_type(
        self,
        value_type_id: int = None,
        value_type_name: str = None,
        value_type_unit: str = None,
    ) -> None:
        """update or add a value type

        Args:
            value_type_id (int, optional): ValueType id to be modified (if None a new ValueType is added), Default to None.
            value_type_name (str, optional): Typename wich should be set or updated. Defaults to None.
            value_type_unit (str, optional): Unit of mesarument wich should be set or updated. Defaults to None.

        Returns:
            _type_: _description_
        """
        with Session(self._engine) as session:
            stmt = select(ValueType).where(ValueType.id == value_type_id)
            db_type = None
            for type in session.scalars(stmt):
                db_type = type
            if db_type is None:
                db_type = ValueType(id=value_type_id)
            if value_type_name:
                db_type.type_name = value_type_name
            elif not db_type.type_name:
                db_type.type_name = "TYPE_%d" % value_type_id
            if value_type_unit:
                db_type.type_unit = value_type_unit
            elif not db_type.type_unit:
                db_type.type_unit = "UNIT_%d" % value_type_id
            session.add_all([db_type])
            session.commit()
            return db_type

    def add_value(self, value_time: int, value_type: int, value_value: float) -> None:
        """Add a measurement point to the database.

        Args:
            value_time (int): unix time stamp of the value.
            value_type (int): Valuetype id of the given value. 
            value_value (float): The measurement value as float.
        """
        with Session(self._engine) as session:
            stmt = select(ValueType).where(ValueType.id == value_type)
            db_type = self.add_or_update_value_type(value_type)
            db_value = Value(time=value_time, value=value_value, value_type=db_type)

            session.add_all([db_type, db_value])
            try:
                session.commit()
            except IntegrityError:
                logging.error("Integrity")
                raise

    def get_value_types(self) -> List[ValueType]:
        """Get all configured value types

        Returns:
            List[ValueType]: List of ValueType objects. 
        """
        with Session(self._engine) as session:
            stmt = select(ValueType)
            return session.scalars(stmt).all()

    def get_value_type(self, value_type_id: int) -> ValueType:
        """Get a special ValueType

        Args:
            value_type_id (int): the primary key of the ValueType

        Returns:
            ValueType: The ValueType object
        """
        with Session(self._engine) as session:
            stmt = select(ValueType).where(ValueType.id == value_type_id)
            return session.scalars(stmt).one()

    def get_values(
        self, value_type_id: int = None, start: int = None, end: int = None
    ) -> List[Value]:
        """Get Values from database.

        The result can be filtered by the following paramater:

        Args:
            value_type_id (int, optional): If set, only value of this given type will be returned. Defaults to None.
            start (int, optional): If set, only values with a timestamp as least as big as start are returned. Defaults to None.
            end (int, optional): If set, only values with a timestamp as most as big as end are returned. Defaults to None.

        Returns:
            List[Value]: _description_
        """
        with Session(self._engine) as session:
            stmt = select(Value)
            if value_type_id is not None:
                stmt = stmt.join(Value.value_type).where(ValueType.id == value_type_id)
            if start is not None:
                stmt = stmt.where(Value.time >= start)
            if end is not None:
                stmt = stmt.where(Value.time <= end)
            stmt = stmt.order_by(Value.time)
            logging.error(start)
            logging.error(stmt)

            return session.scalars(stmt).all()

    def get_device_type(self, device_type_id: int) -> DeviceType:
        """Get a DeviceType by its primary key (id).

        Args:
            device_type_id (int): The primary key of the DeviceType to retrieve.

        Returns:
            DeviceType: The DeviceType object.
        """
        with Session(self._engine) as session:
            stmt = select(DeviceType).where(DeviceType.id == device_type_id)
            device_type = session.scalars(stmt).one_or_none()
            if device_type is None:
                raise NoResultFound(f"DeviceType with id {device_type_id} not found")
            return device_type
    

    def add_or_update_device(
        self,
        device_type_id: int = None,
        device_name: str = None,
        device_location: str = None,
    ) -> DeviceType:
        """Update or add a device type.

        Args:
            device_type_id (int, optional): DeviceType id to be modified (if None, a new DeviceType is added), Defaults to None.
            device_name (str, optional): Name of the device type to be set or updated. Defaults to None.
            device_location (str, optional): Location of the device type to be set or updated. Defaults to None.

        Returns:
            DeviceType: The DeviceType object.
        """
        with Session(self._engine) as session:
            stmt = select(DeviceType).where(DeviceType.id == device_type_id)
            db_device_type = None

            for device_type in session.scalars(stmt):
                db_device_type = device_type

            if db_device_type is None:
                db_device_type = DeviceType(id=device_type_id)

            if device_name:
                db_device_type.name = device_name
            elif not db_device_type.name:
                db_device_type.name = "DEVICE_TYPE_%d" % device_type_id

            if device_location:
                db_device_type.location = device_location
            elif not db_device_type.location:
                db_device_type.location = "DEVICE_LOCATION_%d" % device_type_id

            try:
                session.add(db_device_type)
                session.commit()
            except IntegrityError:
                session.rollback()
                raise

            return db_device_type
