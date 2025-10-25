# analysis/summary.py
from query import cargar_data

def summarize_data():
    df = cargar_data()
    
    print("Total records:", len(df))

    print("\nTemperatura promedio: ")
    print(df.groupby("city")["temp_c"].mean().round(2))
    
    
    print("\n3 Paises mas calorosos:")
    print(df.nlargest(3, "temp_c")[["city", "temp_c"]])

if __name__ == "__main__":
    summarize_data()
