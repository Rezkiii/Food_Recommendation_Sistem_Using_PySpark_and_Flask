from flask import Flask, render_template, request, url_for, flash
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
import csv

app = Flask(__name__)

# Create SparkSession outside the route function for efficiency
spark = SparkSession.builder.appName("FoodDataApp").getOrCreate()

# Function to generate the next Food_ID
def generate_next_food_id(data_path):
    with open(data_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        # Skip the header row
        next(reader)  # Skip the first line (header)
        last_row = max(reader, key=lambda row: int(row[0]))  # Get the last row based on Food_ID
        if last_row:
            return int(last_row[0]) + 1
        else:
            return 1  # Start with 1 if no data exists

@app.route("/", methods=["GET", "POST"])
def food_ratings():
    search_query = request.form.get("search_query")  # Get search query from form

    # Read CSV data within the route function for data freshness
    schema_food = StructType([
        StructField("Food_ID", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("C_Type", StringType(), True),
        StructField("Veg_Non", StringType(), True),
        StructField("Describe", StringType(), True)
    ])

    df_food = spark.read.csv("dataset/1662574418893344.csv", header=True, schema=schema_food)

    # Filter data based on search query (if any)
    if search_query:
        df_filtered = df_food.where(F.col("Name").like(f"%{search_query}%"))
    else:
        df_filtered = df_food  # No filter if no search query

    # Convert DataFrames to lists for easier manipulation in HTML templates
    food_data = df_filtered.toPandas().to_dict('records')

    return render_template("index.html", food_data=food_data)

@app.route("/add_food", methods=["GET", "POST"])
def add_food():
    if request.method == "GET":
        return render_template("add_food.html")
    else:
        # Get new food data from form
        new_name = request.form.get("name")
        new_category = request.form.get("category")
        new_veg_non = request.form.get("veg_non")
        new_describe = request.form.get("describe")

        # Generate the next Food_ID
        next_id = generate_next_food_id("dataset/1662574418893344.csv")

        # Create a new row with the collected data
        new_row = [str(next_id), new_name, new_category, new_veg_non, new_describe]

        # Append the new row to the CSV file using `csv` module
        with open("dataset/1662574418893344.csv", 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(new_row)

        # Success message (optional)
        flash("Food added successfully!", category='success')

        return render_template("add_food.html")
        
@app.route("/food/<food_id>")
def food_details(food_id):
    # Retrieve food details from DataFrame based on ID (implementation depends on your data structure)
    df_food = spark.read.csv("dataset/1662574418893344.csv", header=True)
    food_details = df_food.where(df_food["Food_ID"] == food_id).toPandas().to_dict('records')[0]

    # Implement a simple recommendation system based on category (C_Type) or other relevant attributes
    recommended_food_ids = df_food.where(df_food["C_Type"] == food_details["C_Type"]).select("Food_ID").distinct().limit(3).toPandas()["Food_ID"].tolist()
    recommended_foods = df_food.where(F.col("Food_ID").isin(recommended_food_ids)).select("Name", "Describe").toPandas().to_dict('records')

    return render_template("food_details.html", food_details=food_details, recommended_foods=recommended_foods)

if __name__ == "__main__":
    app.run(debug=True)

