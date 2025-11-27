CREATE TABLE "customer" (
  "customerId" integer PRIMARY KEY,
  "username" varchar(50) UNIQUE NOT NULL,
  "firstName" varchar(50),
  "lastName" varchar(50),
  "DOB" date NOT NULL,
  "address" varchar(200),
  "userReferral" integer
);

CREATE TABLE "department" (
  "departmentId" integer PRIMARY KEY,
  "departmentName" varchar(50) NOT NULL,
  "departmentPhoneNumber" varchar(50),
  "departmentEmail" varchar(50),
  "departmentAddress" varchar(200),
  "departmentManagerId" integer
);

CREATE TABLE "employee" (
  "employeeId" integer PRIMARY KEY,
  "username" varchar(50) UNIQUE NOT NULL,
  "firstName" varchar(50),
  "lastName" varchar(50),
  "DOB" date NOT NULL,
  "phoneNumber" varchar(50),
  "email" varchar(50),
  "address" varchar(200),
  "departmentId" integer,
  "supervisorId" integer
);

CREATE TABLE "manufacture" (
  "manufactureId" integer PRIMARY KEY,
  "manufactureName" varchar(50) NOT NULL,
  "manufacturePhoneNumber" varchar(50),
  "manufactureEmail" varchar(50),
  "manufactureAddress" varchar(200),
  "emergencyContact" varchar(50)
);

CREATE TABLE "product" (
  "productId" integer PRIMARY KEY,
  "productName" varchar(50) NOT NULL,
  "manufactureId" integer,
  "batchOrder" varchar(50),
  "batchOrderDate" date NOT NULL,
  "unitPrice" decimal(10,2),
  "stockQuantity" integer
);

CREATE TABLE "orders" (
  "orderId" integer PRIMARY KEY,
  "customerId" integer,
  "agentId" integer,
  "orderDate" date,
  "totalAmount" decimal(10,2)
);

CREATE TABLE "order_details" (
  "orderDetailId" integer PRIMARY KEY,
  "orderId" integer,
  "productId" integer,
  "quantity" integer,
  "unitPrice" decimal(10,2),
  "lineTotal" decimal(10,2)
);

CREATE TABLE "shipping" (
  "shippingId" integer PRIMARY KEY,
  "orderId" integer UNIQUE,
  "shippingCompany" varchar(50),
  "status" varchar(50),
  "shippingDate" date,
  "deliveryDate" date,
  "trackingNumber" varchar(50)
);

CREATE TABLE "payment" (
  "paymentId" integer PRIMARY KEY,
  "orderId" integer,
  "paymentMethod" varchar(50),
  "paymentStatus" varchar(50),
  "amount" decimal(10,2),
  "paymentDate" date,
  "transactionReference" varchar(100)
);

CREATE TABLE "return_request" (
  "returnId" integer PRIMARY KEY,
  "orderDetailId" integer,
  "reason" varchar(200),
  "returnStatus" varchar(50),
  "refundAmount" decimal(10,2),
  "processedBy" integer,
  "processedDate" date
);

CREATE TABLE "price_history" (
  "priceHistoryId" integer PRIMARY KEY,
  "productId" integer,
  "oldPrice" decimal(10,2),
  "newPrice" decimal(10,2),
  "effectiveDate" date,
  "changedBy" integer
);

ALTER TABLE "customer" ADD FOREIGN KEY ("userReferral") REFERENCES "customer" ("customerId");

ALTER TABLE "department" ADD FOREIGN KEY ("departmentManagerId") REFERENCES "employee" ("employeeId");

ALTER TABLE "employee" ADD FOREIGN KEY ("departmentId") REFERENCES "department" ("departmentId");

ALTER TABLE "employee" ADD FOREIGN KEY ("supervisorId") REFERENCES "employee" ("employeeId");

ALTER TABLE "product" ADD FOREIGN KEY ("manufactureId") REFERENCES "manufacture" ("manufactureId");

ALTER TABLE "orders" ADD FOREIGN KEY ("customerId") REFERENCES "customer" ("customerId");

ALTER TABLE "orders" ADD FOREIGN KEY ("agentId") REFERENCES "employee" ("employeeId");

ALTER TABLE "order_details" ADD FOREIGN KEY ("orderId") REFERENCES "orders" ("orderId");

ALTER TABLE "order_details" ADD FOREIGN KEY ("productId") REFERENCES "product" ("productId");

ALTER TABLE "shipping" ADD FOREIGN KEY ("orderId") REFERENCES "orders" ("orderId");

ALTER TABLE "payment" ADD FOREIGN KEY ("orderId") REFERENCES "orders" ("orderId");

ALTER TABLE "return_request" ADD FOREIGN KEY ("orderDetailId") REFERENCES "order_details" ("orderDetailId");

ALTER TABLE "return_request" ADD FOREIGN KEY ("processedBy") REFERENCES "employee" ("employeeId");

ALTER TABLE "price_history" ADD FOREIGN KEY ("productId") REFERENCES "product" ("productId");

ALTER TABLE "price_history" ADD FOREIGN KEY ("changedBy") REFERENCES "employee" ("employeeId");
