from fastapi import FastAPI
from fastapi.responses import FileResponse
import pandas as pd
import numpy as np


app = FastAPI()

@app.get("/")
async def read_root():
    return FileResponse('templates/index.html')

@app.get("/api/portfolio")
async def get_portfolio():
    # Load the Excel file
    try:
        print("Attempting to read 'INVESTINGS' sheet, skipping 3 rows...")
        df = pd.read_excel('portpolio_r11_py.xlsx', sheet_name='INVESTINGS', skiprows=3)
    except Exception as e:
        print(f"Error reading excel file: {e}")
        df = pd.DataFrame()

    # Drop rows where all elements are NaN
    df.dropna(how='all', inplace=True)

    # Replace infinity and NaN values which are not JSON compliant
    df = df.replace([np.inf, -np.inf], np.nan).astype(object).where(pd.notnull(df), None)

    print("DataFrame Shape:", df.shape)
    print("DataFrame Head:", df.head())
    # Convert dataframe to JSON format
    return df.to_dict(orient='records')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
