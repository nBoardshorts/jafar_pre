# models/metric.py
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
import datetime
from support.base import Base



class Metric(Base):
    """
    Represents a metric in the database. A metric is a numerical value associated with a company
    that provides insight into its financial performance, valuation, and other aspects.
    
    Attributes:
        id (Integer): Primary key for the metric.
        name (String): The name of the metric (e.g., 'Revenue', 'EBITDA').
        description (String): An optional description for the metric.
        source (String): The source of the metric data (e.g., 'eodhistoricaldata.com').
        category_id (Integer): Foreign key referencing the Category_For_Metric table.
        category (relationship): Many-to-one relationship with the Category_For_Metric table.
        metric_values (relationship): One-to-many relationship with the Metric_Value table.
    """
    __tablename__ = 'metrics'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    source= Column(String, nullable=False)
    category_id = Column(Integer, ForeignKey('categories_for_metrics.id'), nullable=True)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_by = Column(String, nullable=False)

    category = relationship("Category_For_Metric", back_populates="metrics")
    metric_values = relationship("Metric_Value", back_populates="metric")