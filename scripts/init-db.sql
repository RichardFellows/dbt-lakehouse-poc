-- init-db.sql: Creates and seeds the lakehouse_source database
-- Idempotent: safe to run multiple times

-- Create database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'lakehouse_source')
BEGIN
    CREATE DATABASE lakehouse_source;
END
GO

USE lakehouse_source;
GO

-- ============================================================
-- customers
-- ============================================================
IF OBJECT_ID('dbo.customers', 'U') IS NOT NULL
    DROP TABLE dbo.customers;
GO

CREATE TABLE dbo.customers (
    customer_id   INT           NOT NULL PRIMARY KEY,
    customer_name NVARCHAR(100) NOT NULL,
    email         NVARCHAR(150) NOT NULL,
    country       NVARCHAR(10)  NOT NULL,
    signup_date   DATE          NOT NULL
);
GO

INSERT INTO dbo.customers (customer_id, customer_name, email, country, signup_date) VALUES
(1, 'Alice Johnson', 'alice@example.com', 'UK', '2023-01-15'),
(2, 'Bob Smith',     'bob@example.com',   'US', '2023-02-20'),
(3, 'Carol White',   'carol@example.com', 'UK', '2023-03-10'),
(4, 'David Brown',   'david@example.com', 'DE', '2023-04-05'),
(5, 'Eve Davis',     'eve@example.com',   'US', '2023-05-22'),
(6, 'Frank Miller',  'frank@example.com', 'FR', '2023-06-18'),
(7, 'Grace Lee',     'grace@example.com', 'UK', '2023-07-30'),
(8, 'Henry Wilson',  'henry@example.com', 'DE', '2023-08-14');
GO

-- ============================================================
-- products
-- ============================================================
IF OBJECT_ID('dbo.products', 'U') IS NOT NULL
    DROP TABLE dbo.products;
GO

CREATE TABLE dbo.products (
    product_id   INT            NOT NULL PRIMARY KEY,
    product_name NVARCHAR(150)  NOT NULL,
    category     NVARCHAR(100)  NOT NULL,
    unit_price   DECIMAL(10, 2) NOT NULL,
    stock_qty    INT            NOT NULL
);
GO

INSERT INTO dbo.products (product_id, product_name, category, unit_price, stock_qty) VALUES
(101, 'Wireless Headphones', 'Electronics',  79.99, 150),
(102, 'Running Shoes',       'Footwear',     59.99, 200),
(103, 'Coffee Maker',        'Kitchen',      49.99,  80),
(104, 'Yoga Mat',            'Sports',       29.99, 300),
(105, 'Mechanical Keyboard', 'Electronics', 129.99,  60),
(106, 'Water Bottle',        'Sports',       19.99, 500),
(107, 'Desk Lamp',           'Home Office',  39.99, 120),
(108, 'Backpack',            'Accessories',  49.99,  90);
GO

-- ============================================================
-- orders  (FK to customers and products)
-- ============================================================
IF OBJECT_ID('dbo.orders', 'U') IS NOT NULL
    DROP TABLE dbo.orders;
GO

CREATE TABLE dbo.orders (
    order_id    INT          NOT NULL PRIMARY KEY,
    customer_id INT          NOT NULL REFERENCES dbo.customers(customer_id),
    product_id  INT          NOT NULL REFERENCES dbo.products(product_id),
    quantity    INT          NOT NULL,
    order_date  DATE         NOT NULL,
    status      NVARCHAR(20) NOT NULL
);
GO

INSERT INTO dbo.orders (order_id, customer_id, product_id, quantity, order_date, status) VALUES
(1001, 1, 101, 1, '2024-01-10', 'completed'),
(1002, 2, 102, 2, '2024-01-12', 'completed'),
(1003, 1, 105, 1, '2024-01-15', 'completed'),
(1004, 3, 103, 1, '2024-01-20', 'completed'),
(1005, 4, 104, 3, '2024-02-01', 'completed'),
(1006, 5, 106, 2, '2024-02-05', 'completed'),
(1007, 2, 107, 1, '2024-02-10', 'completed'),
(1008, 6, 101, 2, '2024-02-14', 'completed'),
(1009, 7, 108, 1, '2024-02-20', 'completed'),
(1010, 3, 102, 1, '2024-02-25', 'shipped'),
(1011, 8, 105, 2, '2024-03-01', 'shipped'),
(1012, 1, 106, 4, '2024-03-05', 'shipped'),
(1013, 5, 103, 1, '2024-03-10', 'pending'),
(1014, 4, 107, 2, '2024-03-12', 'pending'),
(1015, 6, 104, 1, '2024-03-15', 'pending'),
(1016, 7, 101, 1, '2024-03-18', 'completed'),
(1017, 8, 102, 3, '2024-03-20', 'completed'),
(1018, 2, 108, 2, '2024-03-22', 'completed');
GO

-- ============================================================
-- Verify row counts
-- ============================================================
SELECT 'customers' AS tbl, COUNT(*) AS rows FROM dbo.customers
UNION ALL
SELECT 'products',          COUNT(*) FROM dbo.products
UNION ALL
SELECT 'orders',            COUNT(*) FROM dbo.orders;
GO
