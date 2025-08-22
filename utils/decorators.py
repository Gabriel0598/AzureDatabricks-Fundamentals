from datetime import datetime, timedelta
from functools import wraps

def with_date_range(days_back=30):
    """
    Decorator that adds date range calculation to a function.
    
    Args:
        days_back (int): Number of days to look back from the research date.
    
    Returns:
        A decorator function that adds date_range, first_date, and last_date to kwargs.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract research_date_fmt from kwargs or args
            research_date_fmt = kwargs.get('research_date_fmt')
            if not research_date_fmt and len(args) > 1:
                research_date_fmt = args[1]  # Assuming research_date_fmt is the second argument
            
            if not research_date_fmt:
                raise ValueError("research_date_fmt is required")
            
            # Calculate date range
            last_date = research_date_fmt
            end_date = datetime.strptime(last_date, "%Y%m%d")
            first_date = (end_date - timedelta(days=days_back)).strftime("%Y%m%d")
            
            # Generate date range
            dates_range = []
            current_date = datetime.strptime(first_date, "%Y%m%d")
            
            while current_date <= end_date:
                dates_range.append(current_date.strftime("%Y%m%d"))
                current_date += timedelta(days=1)
            
            # Add to kwargs
            kwargs['date_range'] = dates_range
            kwargs['first_date'] = first_date
            kwargs['last_date'] = last_date
            
            # Log date range info
            print(f"Last day of date range: {last_date}")
            print(f"First day of date range for previous {days_back} days: {first_date}")
            print("-" * 50)
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

# For 30-day lookback (default)
@with_date_range()
def process_30day_data(spark, research_date_fmt, **kwargs):
    dates_range = kwargs.get('date_range', [])
    # Process data

# For 7-day lookback
@with_date_range(days_back=7)
def process_weekly_data(spark, research_date_fmt, **kwargs):
    dates_range = kwargs.get('date_range', [])
    # Process data