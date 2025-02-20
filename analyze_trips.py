import pandas as pd
from pathlib import Path
from typing import List

def calculate_daily_averages(data_dir: str = "./data") -> pd.DataFrame:
    """
    Compare trip counts by day of week across different months.
    
    Args:
        data_dir (str): Path to directory containing parquet files
        
    Returns:
        pd.DataFrame: DataFrame with trip counts by month and day of week
    """
    # Get all parquet files in the directory
    parquet_files = list(Path(data_dir).glob("*.parquet"))
    
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")
    
    # List to store dataframes for each file
    all_data = []
    
    for file_path in parquet_files:
        print(f"Processing {file_path.name}...")
        
        # Read parquet file
        df = pd.read_parquet(file_path)
        
        # Convert pickup datetime to datetime if it's not already
        if 'tpep_pickup_datetime' in df.columns:
            date_column = 'tpep_pickup_datetime'
        else:
            date_column = 'pickup_datetime'
            
        df[date_column] = pd.to_datetime(df[date_column])
        
        # Extract day of week and month
        df['day_of_week'] = df[date_column].dt.dayofweek
        df['month'] = df[date_column].dt.month
        
        # Count trips by day and month
        daily_counts = df.groupby(['month', 'day_of_week']).size().reset_index(name='trip_count')
        all_data.append(daily_counts)
    
    # Combine all results
    final_counts = pd.concat(all_data).groupby(['month', 'day_of_week'])['trip_count'].sum().reset_index()
    
    # Convert day numbers to names for better readability
    day_names = {
        0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday',
        4: 'Friday', 5: 'Saturday', 6: 'Sunday'
    }
    
    # Create pivot table for easier comparison
    pivot_counts = final_counts.pivot(index='day_of_week', columns='month', values='trip_count')
    pivot_counts.index = pivot_counts.index.map(day_names)
    pivot_counts.columns = [f'Month {m}' for m in pivot_counts.columns]
    
    return pivot_counts

if __name__ == "__main__":
    try:
        trip_counts = calculate_daily_averages()
        print("\nTrip Counts by Day of Week and Month:")
        print(trip_counts.round(0))  # Round to whole numbers since we're counting trips
        
        # Save to CSV file
        output_file = "daily_trip_counts.csv"
        trip_counts.to_csv(output_file)
        print(f"\nResults saved to {output_file}")
    except Exception as e:
        print(f"Error: {e}") 