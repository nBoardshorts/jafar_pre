# models/category_for_metric.py
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from support.base import Base


class Category_For_Metric(Base):
    """
    Represents a category for a metric in the database. A category groups related metrics together,
    making it easier to filter and analyze metrics based on their category.
    
    Attributes:
        id (Integer): Primary key for the category.
        name (String): The name of the category (e.g., 'Financials', 'Valuation').
        description (String): An optional description for the category.
        metrics (relationship): One-to-many relationship with the Metric table.
    """
    __tablename__ = 'categories_for_metrics'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)

    metrics = relationship("Metric", back_populates="category")
