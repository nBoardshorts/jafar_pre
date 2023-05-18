# models/metric_value.py
from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from support.base import Base


class Metric_Value(Base):
    """
    Represents a metric value in the database. A metric value is a specific instance of a metric
    associated with a company at a given point in time.
    
    Attributes:
        id (Integer): Primary key for the metric value.
        eodhistoricaldata_id (Integer): Foreign key referencing the symbols_eodhistoricaldata table.
        td_ameritrade_id (Integer): Foreign key referencing the symbols_td_ameritrade table.
        metric_id (Integer): Foreign key referencing the Metric table.
        timestamp (DateTime): The date and time when the metric value was recorded.
        value (Float): The numerical value of the metric.
        eodhistoricaldata (relationship): Many-to-one relationship with the Symbol_EODHistoricalData table.
        td_ameritrade (relationship): Many-to-one relationship with the Symbol_TD_Ameritrade table.
        metric (relationship): Many-to-one relationship with the Metric table.
    """
    __tablename__ = 'metric_values'

    id = Column(Integer, primary_key=True)
    eodhistoricaldata_id = Column(Integer, ForeignKey('symbols_eodhistoricaldata.id'), nullable=True)
    td_ameritrade_id = Column(Integer, ForeignKey('symbols_td_ameritrade.id'), nullable=True)
    metric_id = Column(Integer, ForeignKey('metrics.id'), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False)

    eodhistoricaldata = relationship("Symbol_EODHistoricalData")
    td_ameritrade = relationship("Symbol_TD_Ameritrade")
    metric = relationship("Metric", back_populates="metric_values")