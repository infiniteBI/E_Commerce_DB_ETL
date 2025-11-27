from dotenv import load_dotenv
import os
import psycopg2

import random
from datetime import datetime, timedelta
from faker import Faker
import json
import pandas as pd
# Load .env variables
from pathlib import Path

env_path = Path(__file__).parent / 'rdsAuthenticator.env'
load_dotenv(env_path)
load_dotenv('rdsAuthenticator.env')

def get_connection():
     return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_CUSTOMERS = 100
NUM_DEPARTMENTS = 5
NUM_EMPLOYEES = 50
NUM_MANUFACTURES = 5
NUM_PRODUCTS = 10
NUM_ORDERS = 400
NUM_PAYMENTS = 400
NUM_RETURNS = 50
NUM_PRICE_HISTORY = 100

def random_date(start_year=2025, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_dob():
    return random_date(1960, 2005).strftime('%Y-%m-%d')

def generate_customers(n):
    customers = []
    for i in range(1, n + 1):
        customers.append({
            'customerId': i,
            'username': fake.user_name() + str(i),
            'firstName': fake.first_name(),
            'lastName': fake.last_name(),
            'DOB': random_dob(),
            'address': fake.address().replace('\n', ', '),
            'userReferral': random.choice([None] + list(range(1, max(1, i))))
        })
    return customers

def generate_employees(n):
    employees = []
    for i in range(1, n + 1):
        employees.append({
            'employeeId': i,
            'username': fake.user_name() + '_emp' + str(i),
            'firstName': fake.first_name(),
            'lastName': fake.last_name(),
            'DOB': random_dob(),
            'phoneNumber': fake.phone_number(),
            'email': fake.email(),
            'address': fake.address().replace('\n', ', '),
            'departmentId': None,  # Will be set after departments
            'supervisorId': random.choice([None] + list(range(1, max(1, i))))
        })
    return employees

def generate_departments(n, employees):
    departments = []
    for i in range(1, n + 1):
        departments.append({
            'departmentId': i,
            'departmentName': fake.company() + ' Department',
            'departmentPhoneNumber': fake.phone_number(),
            'departmentEmail': fake.company_email(),
            'departmentAddress': fake.address().replace('\n', ', '),
            'departmentManagerId': random.choice([e['employeeId'] for e in employees[:min(20, len(employees))]])
        })
    
    # Assign departments to employees
    for emp in employees:
        emp['departmentId'] = random.randint(1, n)
    
    return departments

def generate_manufactures(n):
    manufactures = []
    for i in range(1, n + 1):
        manufactures.append({
            'manufactureId': i,
            'manufactureName': fake.company(),
            'manufacturePhoneNumber': fake.phone_number(),
            'manufactureEmail': fake.company_email(),
            'manufactureAddress': fake.address().replace('\n', ', '),
            'emergencyContact': fake.phone_number()
        })
    return manufactures

def generate_products(n, manufactures):
    products = []
    for i in range(1, n + 1):
        products.append({
            'productId': i,
            'productName': fake.word().capitalize() + ' ' + fake.word().capitalize(),
            'manufactureId': random.choice([m['manufactureId'] for m in manufactures]),
            'batchOrder': f"BATCH-{random.randint(1000, 9999)}",
            'batchOrderDate': random_date(2022, 2024).strftime('%Y-%m-%d'),
            'unitPrice': round(random.uniform(5.0, 500.0), 2),
            'stockQuantity': random.randint(0, 1000)
        })
    return products

def generate_orders(n, customers, employees):
    orders = []
    for i in range(1, n + 1):
        orders.append({
            'orderId': i,
            'customerId': random.choice([c['customerId'] for c in customers]),
            'agentId': random.choice([e['employeeId'] for e in employees]),
            'orderDate': random_date(2023, 2024).strftime('%Y-%m-%d'),
            'totalAmount': 0  # Will be calculated from order_details
        })
    return orders

def generate_order_details(orders, products):
    order_details = []
    detail_id = 1
    
    for order in orders:
        num_items = random.randint(1, 5)
        order_total = 0
        
        for i in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 10)
            unit_price = product['unitPrice']
            line_total = round(quantity * unit_price, 2)
            order_total += line_total
            
            order_details.append({
                'orderDetailId': detail_id,
                'orderId': order['orderId'],
                'productId': product['productId'],
                'quantity': quantity,
                'unitPrice': unit_price,
                'lineTotal': line_total
            })
            detail_id += 1
        
        order['totalAmount'] = round(order_total, 2)
    
    return order_details

def generate_shipping(orders):
    shipping = []
    statuses = ['Pending', 'Shipped', 'In Transit', 'Delivered', 'Cancelled']
    companies = ['FedEx', 'UPS', 'DHL', 'USPS', 'Amazon Logistics']
    
    for order in orders:
        ship_date = datetime.strptime(order['orderDate'], '%Y-%m-%d')
        delivery_date = ship_date + timedelta(days=random.randint(2, 14))
        
        shipping.append({
            'shippingId': order['orderId'],
            'orderId': order['orderId'],
            'shippingCompany': random.choice(companies),
            'status': random.choice(statuses),
            'shippingDate': ship_date.strftime('%Y-%m-%d'),
            'deliveryDate': delivery_date.strftime('%Y-%m-%d'),
            'trackingNumber': f"TRK{random.randint(100000000, 999999999)}"
        })
    
    return shipping

def generate_payments(orders):
    payments = []
    methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash']
    statuses = ['Completed', 'Pending', 'Failed', 'Refunded']
    
    for i, order in enumerate(orders, 1):
        payment_date = datetime.strptime(order['orderDate'], '%Y-%m-%d') + timedelta(days=random.randint(0, 2))
        
        payments.append({
            'paymentId': i,
            'orderId': order['orderId'],
            'paymentMethod': random.choice(methods),
            'paymentStatus': random.choice(statuses),
            'amount': order['totalAmount'],
            'paymentDate': payment_date.strftime('%Y-%m-%d'),
            'transactionReference': f"TXN-{random.randint(1000000, 9999999)}"
        })
    
    return payments

def generate_returns(n, order_details, employees):
    returns = []
    reasons = ['Defective', 'Wrong item', 'Not as described', 'Changed mind', 'Damaged in shipping']
    statuses = ['Pending', 'Approved', 'Rejected', 'Processed']
    
    selected_details = random.sample(order_details, min(n, len(order_details)))
    
    for i, detail in enumerate(selected_details, 1):
        returns.append({
            'returnId': i,
            'orderDetailId': detail['orderDetailId'],
            'reason': random.choice(reasons),
            'returnStatus': random.choice(statuses),
            'refundAmount': round(detail['lineTotal'] * random.uniform(0.5, 1.0), 2),
            'processedBy': random.choice([e['employeeId'] for e in employees]),
            'processedDate': random_date(2025).strftime('%Y-%m-%d')
        })
    
    return returns

def generate_price_history(n, products, employees):
    price_history = []
    
    for i in range(1, n + 1):
        product = random.choice(products)
        old_price = product['unitPrice']
        new_price = round(old_price * random.uniform(0.8, 1.2), 2)
        
        price_history.append({
            'priceHistoryId': i,
            'productId': product['productId'],
            'oldPrice': old_price,
            'newPrice': new_price,
            'effectiveDate': random_date(2023, 2024).strftime('%Y-%m-%d'),
            'changedBy': random.choice([e['employeeId'] for e in employees])
        })
    
    return price_history

def generate_sql_inserts(table_name, data):
    if not data:
        return ""
    conn = get_connection()
    cursor = conn.cursor()
    columns = list(data[0].keys())
    sql = f"-- Insert data for {table_name}\n"
    
    for row in data:
        values = []
        for col in columns:
            val = row[col]
            if val is None:
                values.append('NULL')
            elif isinstance(val, str):
                values.append(f"'{val.replace('\'', '\'\'')}'")
            else:
                values.append(str(val))
        
        sql += f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
    
    sql += "\n"
    return sql

def insert_data_into_rds(table_name, data):
    
    if not data:
        print("No data to insert.")
        return
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Drop constraints
        print("Dropping constraints...")
        cursor.execute("ALTER TABLE employee DROP CONSTRAINT IF EXISTS fk_employee_department")
        cursor.execute("ALTER TABLE department DROP CONSTRAINT IF EXISTS fk_department_manager")
        '''
        # 2. Clear existing data (optional)
        print("Clearing existing data...")
        cursor.execute("DELETE FROM employee")
        cursor.execute("DELETE FROM department")
        '''
        #3. inserting data
        columns = list(data[0].keys())
        quoted_col_names = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {table_name} ({quoted_col_names}) VALUES ({placeholders}) ON CONFLICT ("{primary_key}") DO NOTHING"

        for row in data:
            values = [row[col] for col in columns]
            cursor.execute(sql, values)

        # 4. Re-add constraints
        print("Re-adding constraints...")
        cursor.execute("""
            ALTER TABLE employee 
            ADD CONSTRAINT fk_employee_department 
            FOREIGN KEY (departmentID) REFERENCES department(departmentID)
        """)
        cursor.execute("""
            ALTER TABLE department 
            ADD CONSTRAINT fk_department_manager 
            FOREIGN KEY (managerID) REFERENCES employee(employeeID)
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"✗ Error: {e}")
    finally:
        cursor.close()
        conn.close()

    print(f"Inserted {len(data)} rows into {table_name}")

# Generate all data
print("Generating data...")
customers = generate_customers(NUM_CUSTOMERS)
employees = generate_employees(NUM_EMPLOYEES)
departments = generate_departments(NUM_DEPARTMENTS, employees)
manufactures = generate_manufactures(NUM_MANUFACTURES)
products = generate_products(NUM_PRODUCTS, manufactures)
orders = generate_orders(NUM_ORDERS, customers, employees)
order_details = generate_order_details(orders, products)
shipping = generate_shipping(orders)
payments = generate_payments(orders)
returns = generate_returns(NUM_RETURNS, order_details, employees)
price_history = generate_price_history(NUM_PRICE_HISTORY, products, employees)

#inserting data into aws
if __name__ == "__main__":
    insert_data_into_rds('customer', customers)
    insert_data_into_rds('employee', employees)
    insert_data_into_rds('department', departments)
    insert_data_into_rds('manufacture', manufactures)
    insert_data_into_rds('product', products)
    insert_data_into_rds('orders', orders)
    insert_data_into_rds('order_details', order_details)
    insert_data_into_rds('shipping', shipping)
    insert_data_into_rds('payment', payments)
    insert_data_into_rds('return_request', returns)
    insert_data_into_rds('price_history', price_history)



# Generate SQL script file
print("Generating SQL file...")
with open('database_inserts.sql', 'w', encoding='utf-8') as f:
    f.write("-- Generated Database Insert Statements\n\n")
    f.write(generate_sql_inserts('customer', customers))
    f.write(generate_sql_inserts('employee', employees))
    f.write(generate_sql_inserts('department', departments))
    f.write(generate_sql_inserts('manufacture', manufactures))
    f.write(generate_sql_inserts('product', products))
    f.write(generate_sql_inserts('orders', orders))
    f.write(generate_sql_inserts('order_details', order_details))
    f.write(generate_sql_inserts('shipping', shipping))
    f.write(generate_sql_inserts('payment', payments))
    f.write(generate_sql_inserts('return_request', returns))
    f.write(generate_sql_inserts('price_history', price_history))

'''
# Generate CSV file
print("Generating csv file...")
all_data = {
    'customers': customers,
    'employees': employees,
    'departments': departments,
    'manufactures': manufactures,
    'products': products,
    'orders': orders,
    'order_details': order_details,
    'shipping': shipping,
    'payments': payments,
    'returns': returns,
    'price_history': price_history
}


for table_name, data in all_data.items():
    if data:  
        df = pd.DataFrame(data)
        file_path = f"csv_exports/{table_name}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"{table_name}.csv generated at {file_path}")
'''


print("Done! Files created:")
print("- database_inserts.sql (SQL INSERT statements)")
#print("- database_data.csv (csv format)")
print(f"\nGenerated:")
print(f"  {len(customers)} customers")
print(f"  {len(employees)} employees")
print(f"  {len(departments)} departments")
print(f"  {len(manufactures)} manufactures")
print(f"  {len(products)} products")
print(f"  {len(orders)} orders")
print(f"  {len(order_details)} order details")
print(f"  {len(shipping)} shipping records")
print(f"  {len(payments)} payments")
print(f"  {len(returns)} return requests")
print(f"  {len(price_history)} price history records")
