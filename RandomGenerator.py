from dotenv import load_dotenv
import os
import psycopg2
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
from pathlib import Path
import traceback

# Load .env variables
env_path = Path(__file__).parent / 'rdsAuthenticator.env'
load_dotenv(env_path)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

fake = Faker()
# Comment out seeds for different data each run
# Faker.seed(42)
# random.seed(42)

# Configuration
NUM_CUSTOMERS = 50
NUM_DEPARTMENTS = 5
NUM_EMPLOYEES = 30
NUM_MANUFACTURES = 5
NUM_PRODUCTS = 10
NUM_ORDERS = 200
NUM_PAYMENTS = 200
NUM_RETURNS = 26
NUM_PRICE_HISTORY = 60

# Generate batch ID once
BATCH_ID = datetime.now().strftime('%Y%m%d_%H%M%S')

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
            'departmentId': None,
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
            'totalAmount': 0
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

# Add tracking columns (run ONCE, then comment out)
def add_tracking_columns():
    """Run this function ONCE to add batch_id and time_updated columns"""
    conn = get_connection()
    cursor = conn.cursor()
    
    tables = ['customer', 'employee', 'department', 'manufacture', 
              'product', 'orders', 'order_details', 'shipping', 
              'payment', 'return_request', 'price_history']
    
    print("\n" + "="*60)
    print("ADDING TRACKING COLUMNS")
    print("="*60)
    
    for table in tables:
        try:
            cursor.execute(f"""
                ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS batch_id VARCHAR(50) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS time_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """)
            conn.commit()
            print(f"✓ Added tracking columns to {table}")
        except Exception as e:
            conn.rollback()
            print(f"✗ Error on {table}: {e}")
    
    cursor.close()
    conn.close()
    print("="*60)

# Drop ALL foreign key constraints ONCE
def drop_all_foreign_keys():
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        print("\n" + "="*60)
        print("DROPPING ALL FOREIGN KEY CONSTRAINTS")
        print("="*60)
        
        # Query to get all foreign key constraints
        cursor.execute("""
            SELECT 
                tc.table_name, 
                tc.constraint_name
            FROM information_schema.table_constraints tc
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
        """)
        
        constraints = cursor.fetchall()
        
        if not constraints:
            print("No foreign key constraints found")
        
        for table_name, constraint_name in constraints:
            try:
                cursor.execute(f'ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS "{constraint_name}"')
                print(f" Dropped {constraint_name} from {table_name}")
            except Exception as e:
                print(f"✗ Failed to drop {constraint_name}: {e}")
        
        conn.commit()
        print("="*60)
        print(f"✓ Dropped {len(constraints)} constraints")
        print("="*60)
        
    except Exception as e:
        conn.rollback()
        print(f"Error dropping constraints: {e}")
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()

# Re-create ALL foreign key constraints ONCE
def recreate_all_foreign_keys():
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        print("\n" + "="*60)
        print("RE-CREATING FOREIGN KEY CONSTRAINTS")
        print("="*60)
        
        constraints = [
            # Customer constraints
            ('customer', 'fk_customer_referral', 
             'ALTER TABLE customer ADD CONSTRAINT fk_customer_referral FOREIGN KEY (userReferral) REFERENCES customer(customerId)'),
            
            # Employee constraints
            ('employee', 'fk_employee_department',
             'ALTER TABLE employee ADD CONSTRAINT fk_employee_department FOREIGN KEY (departmentId) REFERENCES department(departmentId)'),
            ('employee', 'fk_employee_supervisor',
             'ALTER TABLE employee ADD CONSTRAINT fk_employee_supervisor FOREIGN KEY (supervisorId) REFERENCES employee(employeeId)'),
            
            # Department constraints
            ('department', 'fk_department_manager',
             'ALTER TABLE department ADD CONSTRAINT fk_department_manager FOREIGN KEY (departmentManagerId) REFERENCES employee(employeeId)'),
            
            # Product constraints
            ('product', 'fk_product_manufacture',
             'ALTER TABLE product ADD CONSTRAINT fk_product_manufacture FOREIGN KEY (manufactureId) REFERENCES manufacture(manufactureId)'),
            
            # Orders constraints
            ('orders', 'fk_orders_customer',
             'ALTER TABLE orders ADD CONSTRAINT fk_orders_customer FOREIGN KEY (customerId) REFERENCES customer(customerId)'),
            ('orders', 'fk_orders_agent',
             'ALTER TABLE orders ADD CONSTRAINT fk_orders_agent FOREIGN KEY (agentId) REFERENCES employee(employeeId)'),
            
            # Order details constraints
            ('order_details', 'fk_order_details_order',
             'ALTER TABLE order_details ADD CONSTRAINT fk_order_details_order FOREIGN KEY (orderId) REFERENCES orders(orderId)'),
            ('order_details', 'fk_order_details_product',
             'ALTER TABLE order_details ADD CONSTRAINT fk_order_details_product FOREIGN KEY (productId) REFERENCES product(productId)'),
            
            # Shipping constraints
            ('shipping', 'fk_shipping_order',
             'ALTER TABLE shipping ADD CONSTRAINT fk_shipping_order FOREIGN KEY (orderId) REFERENCES orders(orderId)'),
            
            # Payment constraints
            ('payment', 'fk_payment_order',
             'ALTER TABLE payment ADD CONSTRAINT fk_payment_order FOREIGN KEY (orderId) REFERENCES orders(orderId)'),
            
            # Return request constraints
            ('return_request', 'fk_return_order_detail',
             'ALTER TABLE return_request ADD CONSTRAINT fk_return_order_detail FOREIGN KEY (orderDetailId) REFERENCES order_details(orderDetailId)'),
            ('return_request', 'fk_return_processed_by',
             'ALTER TABLE return_request ADD CONSTRAINT fk_return_processed_by FOREIGN KEY (processedBy) REFERENCES employee(employeeId)'),
            
            # Price history constraints
            ('price_history', 'fk_price_history_product',
             'ALTER TABLE price_history ADD CONSTRAINT fk_price_history_product FOREIGN KEY (productId) REFERENCES product(productId)'),
            ('price_history', 'fk_price_history_changed_by',
             'ALTER TABLE price_history ADD CONSTRAINT fk_price_history_changed_by FOREIGN KEY (changedBy) REFERENCES employee(employeeId)'),
        ]
        
        success_count = 0
        for table_name, constraint_name, sql in constraints:
            try:
                cursor.execute(sql)
                print(f"✓ Added {constraint_name} to {table_name}")
                success_count += 1
            except Exception as e:
                print(f"✗ Failed to add {constraint_name}: {e}")
        
        conn.commit()
        print("="*60)
        print(f"✓ Re-created {success_count}/{len(constraints)} constraints")
        print("="*60)
        
    except Exception as e:
        conn.rollback()
        print(f"Error adding constraints: {e}")
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()

def insert_data_into_rds(table_name, data, batch_id):
    """Insert data into RDS with detailed debugging"""
    PRIMARY_KEYS = {
        'customer': 'customerId',
        'employee': 'employeeId',
        'department': 'departmentId',
        'manufacture': 'manufactureId',
        'product': 'productId',
        'orders': 'orderId',
        'order_details': 'orderDetailId',
        'shipping': 'shippingId',
        'payment': 'paymentId',
        'return_request': 'returnId',
        'price_history': 'priceHistoryId'
    }
    
    if not data:
        print(f"No data to insert for {table_name}.")
        return
    
    primary_key = PRIMARY_KEYS.get(table_name)
    if primary_key is None:
        print(f" No primary key defined for table {table_name}. Skipping...")
        return
    
    print(f"\n--- Processing {table_name} ---")
    print(f"Records to insert: {len(data)}")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Add batch_id and time_updated BEFORE getting columns
        timestamp = datetime.now()
        for row in data:
            row['batch_id'] = batch_id
            row['time_updated'] = timestamp
        
        # Get columns (includes batch_id and time_updated)
        columns = list(data[0].keys())
        print(f"Columns ({len(columns)}): {', '.join(columns[:5])}...")
        
        quoted_col_names = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f'INSERT INTO {table_name} ({quoted_col_names}) VALUES ({placeholders}) ON CONFLICT ("{primary_key}") DO NOTHING'
        
        print(f"SQL Preview: INSERT INTO {table_name} (...{len(columns)} columns...) VALUES (...) ON CONFLICT...")
        
        inserted_count = 0
        failed_rows = []
        
        for i, row in enumerate(data):
            values = [row[col] for col in columns]
            try:
                cursor.execute(sql, values)
                inserted_count += cursor.rowcount
            except Exception as row_error:
                failed_rows.append((i+1, str(row_error)[:100]))
                if len(failed_rows) <= 3:  # Show first 3 errors
                    print(f"  Row {i+1} failed: {row_error}")
        
        conn.commit()
        skipped_count = len(data) - inserted_count - len(failed_rows)
        
        print(f" {table_name}: Inserted {inserted_count} | Skipped {skipped_count} | Failed {len(failed_rows)}")
        
        if len(failed_rows) > 3:
            print(f"  ... and {len(failed_rows) - 3} more errors")
        
    except Exception as e:
        conn.rollback()
        print(f" FATAL ERROR inserting into {table_name}: {e}")
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()

def generate_sql_inserts(table_name, data):
    """Generate SQL INSERT statements for backup"""
    if not data:
        return ""
    
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

# Main execution
if __name__ == "__main__":
    print("\n" + "="*60)
    print("ETL PROCESS STARTING")
    print(f"Batch ID: {BATCH_ID}")
    print("="*60)
    
    # STEP 0: Add tracking columns (UNCOMMENT AND RUN ONCE, then comment out)
    # add_tracking_columns()
    
    # STEP 1: Generate data
    print("\nSTEP 1: GENERATING DATA")
    print("-"*60)
    
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
    
    print("Data generation complete")
    
    # STEP 2: Drop ALL constraints ONCE
    drop_all_foreign_keys()
    
    # STEP 3: Insert data in dependency order
    print("\nSTEP 3: INSERTING DATA INTO RDS")
    print("-"*60)
    
    # Tables without dependencies first
    insert_data_into_rds('customer', customers, BATCH_ID)
    insert_data_into_rds('manufacture', manufactures, BATCH_ID)
    
    # Then tables that depend on above
    insert_data_into_rds('employee', employees, BATCH_ID)
    insert_data_into_rds('department', departments, BATCH_ID)
    insert_data_into_rds('product', products, BATCH_ID)
    
    # Then orders and related
    insert_data_into_rds('orders', orders, BATCH_ID)
    insert_data_into_rds('order_details', order_details, BATCH_ID)
    insert_data_into_rds('shipping', shipping, BATCH_ID)
    insert_data_into_rds('payment', payments, BATCH_ID)
    insert_data_into_rds('return_request', returns, BATCH_ID)
    insert_data_into_rds('price_history', price_history, BATCH_ID)
    
    # STEP 4: Re-add ALL constraints ONCE
    recreate_all_foreign_keys()
    
    # STEP 5: Generate SQL backup file
    print("\nSTEP 5: GENERATING SQL BACKUP FILE")
    print("-"*60)
    
    try:
        with open('database_inserts.sql', 'w', encoding='utf-8') as f:
            f.write(f"-- Generated Database Insert Statements\n")
            f.write(f"-- Batch ID: {BATCH_ID}\n")
            f.write(f"-- Generated: {datetime.now()}\n\n")
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
        print(" database_inserts.sql created")
    except Exception as e:
        print(f"Error generating SQL file: {e}")
    
    # STEP 6: Generate CSV files
    print("\nSTEP 6: GENERATING CSV FILES")
    print("-"*60)
    
    try:
        # Create csv_exports folder if it doesn't exist
        csv_folder = Path('csv_exports')
        csv_folder.mkdir(exist_ok=True)
        
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
                file_path = csv_folder / f"{table_name}.csv"
                df.to_csv(file_path, index=False, encoding='utf-8')
                print(f" {table_name}.csv created ({len(data)} records)")
        
        print(f" All CSV files saved to {csv_folder}/")
    except Exception as e:
        print(f" Error generating CSV files: {e}")
        traceback.print_exc()
    
    # Summary
    print("\n" + "="*60)
    print("ETL PROCESS COMPLETE!")
    print("="*60)
    print(f"Batch ID: {BATCH_ID}")
    print(f"\nData Summary:")
    print(f"  • {len(customers):3d} customers")
    print(f"  • {len(employees):3d} employees")
    print(f"  • {len(departments):3d} departments")
    print(f"  • {len(manufactures):3d} manufactures")
    print(f"  • {len(products):3d} products")
    print(f"  • {len(orders):3d} orders")
    print(f"  • {len(order_details):3d} order details")
    print(f"  • {len(shipping):3d} shipping records")
    print(f"  • {len(payments):3d} payments")
    print(f"  • {len(returns):3d} return requests")
    print(f"  • {len(price_history):3d} price history records")
    print("="*60)
