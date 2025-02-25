import streamlit as st
import mysql.connector
import pandas as pd

# Function to connect to the MySQL database
def connect_to_database():
    try:
        mydb = mysql.connector.connect(
            host="gateway01.ap-southeast-1.prod.aws.tidbcloud.com",  
            user="2k28pcyNk66J4wT.root",  
            password="MJfUrQWyu2HIZEBk",  
            database="Zomata"  
        )
        return mydb
    except mysql.connector.Error as err:
        st.error(f"Database connection error, check yor database server: {err}")
        return None

# Function to execute a SQL query
def execute_query(mydb, query, fetch=True):
    try:
        cursor = mydb.cursor()
        cursor.execute(query)
        if fetch:
            result = cursor.fetchall() # Fetch all rows from the query result
            columns = [col[0] for col in cursor.description] 
            return result, columns
        else:
            mydb.commit()
            return None, None
    except mysql.connector.Error as err:
        raise err  # Raise the error to be handled in the main function

# Function to get list of tables in the database
def get_tables(mydb):
    query = "SHOW TABLES"
    result, columns = execute_query(mydb, query)
    tables = [row[0] for row in result]  # Extract table names from the result
    return tables

# Function to get column names for a table
def get_columns(mydb, table):
    query = f"SHOW COLUMNS FROM {table}"
    result, columns = execute_query(mydb, query)
    columns = [row[0] for row in result]  # Extract column names from the result
    return columns

# Function to create a new record
def create_record(mydb, table, columns, values):
    try:
        cursor = mydb.cursor()
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"
        cursor.execute(query, values)  # Pass the values as a tuple
        mydb.commit()
        new_id = cursor.lastrowid
        st.success(f"Record created successfully! New ID: {new_id}")
        
    except mysql.connector.Error as err:
        st.error(f"Database error: {err}")
    except Exception as e:
        st.error(f"Error: {e}")
    

# Function to read records
def read_records(mydb, table):
    query = f"SELECT * FROM {table}"
    result, columns = execute_query(mydb, query)
    return result, columns

# Function to update a record
def update_record(mydb, table, column, new_value, condition):
    try:
        if not condition.strip():
            raise ValueError("WHERE condition cannot be empty.")
        query = f"UPDATE {table} SET {column} = %s WHERE {condition}"
        cursor = mydb.cursor()
        cursor.execute(query, (new_value,))
        mydb.commit()
        st.success("Record updated successfully!")
    except mysql.connector.Error as err:
        st.error(f"Database error: {err}")
    except ValueError as ve:
        st.error(f"Validation error: {ve}")

# Function to delete a record
def delete_record(mydb, table, condition):
    try:
        if not condition.strip():
            raise ValueError("WHERE condition cannot be empty.")
        query = f"DELETE FROM {table} WHERE {condition}"
        cursor = mydb.cursor()
        cursor.execute(query)
        mydb.commit()
        st.success("Record deleted successfully!")
    except mysql.connector.Error as err:
        st.error(f"Database error: {err}")
    except ValueError as ve:
        st.error(f"Validation error: {ve}")

# Home Page
def home_page():
    st.title("Welcome to Zomato Food Delivery")
    st.write("Explore delicious food delivered to your doorstep!")

    # Display Zomato food delivery images
    st.image("https://i0.wp.com/tejimandi.com/wp-content/uploads/2022/06/61a81b49-9c79-4b6a-ae4f-d212aee39621-photo-1.png?resize=1536%2C804&ssl=1", caption="Zomato Delivery",  use_container_width=True)
    st.image("https://assets.vogue.com/photos/63d169f727f1d528635b4287/master/w_1920,c_limit/GettyImages-1292563627.jpg", caption="Zomato Food",  use_container_width=True)
    

# Main Streamlit app
def main():
    # Sidebar for navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Home", "CRUD Operations"])

    if page == "Home":
        home_page()
    elif page == "CRUD Operations":
        st.title("CRUD Operations")


    # Connect to the database
    mydb = connect_to_database()

    # Get list of tables in the database
    tables = get_tables(mydb)

    # Sidebar for CRUD operations
    st.sidebar.header("CRUD Operations")
    operation = st.sidebar.selectbox(
        "Choose Operation",
        ["Query Executor", "Create", "Read", "Update", "Delete"]
    )

    if operation == "Query Executor":
        # List of 20 SQL queries
        queries = [
            #Q1
            """SELECT DAYNAME(order_date) AS day_name, COUNT(order_id) AS total_orders
            FROM orders
            GROUP BY day_name
            ORDER BY total_orders DESC;""",
            #Q2
            """ select c.location, count(order_id) as total_orders
            from orders o
            join customers c on o.customer_id = c.customer_id
            group by c.location
            order by total_orders desc limit 10; """, 
            #Q3
             """ select order_id, customer_id, restaurant_id, order_date,
            status
            from orders 
            where status = 'Cancelled'; """,
            #Q4
            """ select d.order_id, d.delivery_person_id,d.delivery_time as actual_time,
            estimated_time,
            (d.delivery_time - d.estimated_time) as dealy_minutes
            from deliveries d
            where d.delivery_time > d.estimated_time
            order by dealy_minutes desc;""",
            #Q5
            """ select d.delivery_person_id, count(d.delivery_id) as dealy_deliveries
            from deliveries d 
            where d.delivery_time > d.estimated_time
            group by d.delivery_person_id
            order by dealy_deliveries desc;  """,
            #Q6
            """ select 
	        o.restaurant_id,
	        r.name as Restaurant,
            count(order_id) as num_cancel_order
            from orders o join restaurants r on o.restaurant_id = r.restaurant_id 
            where o.status = 'Cancelled'
            group by r.name, o.restaurant_id
            order by num_cancel_order desc; """,
            #Q7
            """ select c.customer_id,c.name as customer_name, count(order_id) as total_orders 
            from orders o 
            join customers c on o.customer_id = c.customer_id 
            group by c.customer_id,c.name
            order by total_orders desc limit 5;  """,
            #Q8
            """ select preferred_cuisine,count(*) as customer_count
            from customers
            group by preferred_cuisine
            order by customer_count
            desc limit 6;  """,
            #Q9
            """select customer_id,sum(total_amount) as Total_spent_Amount
            from orders
            group by customer_id
            order by Total_spent_Amount desc limit 10;""",
            #Q10
            """ select vehicle_type,avg(delivery_time) as avg_delivery_time 
            from deliveries group by vehicle_type order by avg_delivery_time desc; """,
            #Q11
            """ select delivery_person_id,count(*) as total_deliveries
            from deliveries group by delivery_person_id order by total_deliveries desc limit 10;""",
            #Q12
            """ select vehicle_type, count(*) as usage_count 
            from deliveries group by vehicle_type order by usage_count desc; """,
            #Q13
            """ select r.name,count(o.order_id) as total_orders 
            from orders o 
            join restaurants r on o.restaurant_id = r.restaurant_id group by r.name 
            order by total_orders desc limit 10;  """,
            #Q14
            """ select r.name,avg(o.total_amount) as avg_order_value 
            from orders o 
            join restaurants r on o.restaurant_id = r.restaurant_id 
            group by r.name order by avg_order_value desc limit 10;""",
            #Q15
            """ select name,rating from restaurants order by rating desc limit 10;""",
            #Q16
            """ select count(*) as Active_restaurants from restaurants where is_active = 1; """,
            #Q17
            """ select payment_mode,count(*) as usage_count 
            from orders group by payment_mode order by usage_count desc;""",
            #Q18
            """ select avg(discount_applied) as avg_discount from orders;  """,
            #Q19
            """ select location,count(*) as total_restaurants 
            from restaurants group by location order by total_restaurants desc limit 15;""",
            #Q20
            """ select avg(rating) as overall_avg_rating from restaurants; """
 ]

        query_descriptions = [
            "Find Peak Ordering day",
            "Find Peak Ordering Locations",
            "Find Cancelled Orders",
            "Find delayed delivers",
            "Find number of dealy deliveries in delivery persons",
            "Find Restaurants most cancelled order",
            "Find the top 5 customers by total orders",
            "Find the most preferred cuisine type",
            "Find the top 5 customers by total spending",
            "Find average delivery time for each vehicle type",
            "Find the best delivery personnel (most deliveries completed",
            "Find the most frequently used delivery vehicle",
            "Find the most popular restaurant by orders",
            "Find the restaurant with the highest average order value",
            "Find restaurants with the highest rating",
            "Find the active restaurants on the platform",
            "Find Most used payment mode",
            "Find average discount applied to orders",
            "Find top 15 locations with highest restaurant present",
            "Find the overall average restaurant rating",
]

        # Create a sidebar selectbox for query selection
        selected_query = st.sidebar.selectbox(
            "Select a Query",
            options=[f"Query {i+1}" for i in range(len(queries))],  # Options: Query 1, Query 2, ..., Query 20
            format_func=lambda x: f"{x}: {query_descriptions[int(x.split(' ')[1]) - 1]}"  # Show descriptions in the dropdown
        )

        # Get the index of the selected query
        query_index = int(selected_query.split(" ")[1]) - 1

        # Display the selected query and its description
        st.write(f"### {selected_query}")
        st.write(f"**Description:** {query_descriptions[query_index]}")
        st.write(f"**SQL Query:** `{queries[query_index]}`")

        # Execute the selected query
        try:
            result, columns = execute_query(mydb, queries[query_index])   # Execute the selected query
            st.write("**Result:**")
            st.table(pd.DataFrame(result, columns=columns))  # Display the result as a table with column names
        except mysql.connector.Error as err:
            st.error(f"Error: {err}")  # Display any errors that occur

    elif operation == "Create":
        st.header("Create a New Record")
        table = st.selectbox("Select Table", tables)  # Dropdown for table selection
        columns = st.text_input("Columns (comma-separated)")
        values = st.text_input("Values (comma-separated)")
        if st.button("Create"):
            if table and columns and values:
                columns = [col.strip() for col in columns.split(",")]
                values = [val.strip() for val in values.split(",")]
                create_record(mydb, table, columns, values)
            else:
                st.error("Please fill in all fields.")

    elif operation == "Read":
        st.header("Read Records")
        table = st.selectbox("Select Table", tables)  # Dropdown for table selection
        if st.button("Read"):
            if table:
                result, columns = read_records(mydb, table)
                st.write("**Records:**")
                st.table(pd.DataFrame(result, columns=columns))  # Display the result as a table with column names
                
            else:
                st.error("Please provide a table name.")

    elif operation == "Update":
        st.header("Update a Record")
        table = st.selectbox("Select Table", tables)  # Dropdown for table selection
        if table:
            columns = get_columns(mydb, table)  # Fetch column names for the selected table
            column = st.selectbox("Select Column to Update", columns)  # Dropdown for column selection
            new_value = st.text_input(f"New Value for {column}")
            condition = st.text_input("WHERE condition (e.g., id = 1)")
            if st.button("Update"):
                if table and column and new_value and condition:
                    if "=" not in condition:
                        st.error("Invalid WHERE condition. It must include an '=' operator (e.g., id = 1).")
                    else:
                        update_record(mydb, table, column, new_value, condition)
                else:
                    st.error("Please fill in all fields.")

    elif operation == "Delete":
        st.header("Delete a Record")
        table = st.selectbox("Select Table", tables)  # Dropdown for table selection
        condition = st.text_input("WHERE condition (e.g., id = 1)")
        if st.button("Delete"):
            if table and condition:
                delete_record(mydb, table, condition)
            else:
                st.error("Please fill in all fields.")

# Run the app
if __name__ == "__main__":
    main()