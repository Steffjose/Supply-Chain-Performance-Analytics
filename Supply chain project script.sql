CREATE DATABASE supply_chain_db;
USE supply_chain_db;
CREATE TABLE suppliers (
    supplier_id VARCHAR(50) PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    location VARCHAR(100),
    rating DECIMAL(3, 2)
); 
CREATE TABLE calendar (
    date DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    week INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week VARCHAR(20) NOT NULL
);
CREATE TABLE sales_forecast (
    date DATE NOT NULL,
    sku VARCHAR(50) NOT NULL,
    historical_demand INTEGER,
    forecasted_demand INTEGER   
);
CREATE TABLE production (
    production_id VARCHAR(50) PRIMARY KEY,
    date DATE NOT NULL,
    sku VARCHAR(50) NOT NULL,
    output_quantity INTEGER NOT NULL,
    downtime_hours NUMERIC(10, 2),
    downtime_reason VARCHAR(255) 
);
CREATE TABLE inventory (
    sku VARCHAR(50) NOT NULL,
    warehouse_id VARCHAR(50) NOT NULL,
    supplier_id VARCHAR(50) NOT NULL,
    current_stock INTEGER NOT NULL,
    reorder_level INTEGER NOT NULL
);
CREATE TABLE purchase_orders (
    purchase_order_id VARCHAR(50) PRIMARY KEY,
    order_date DATE NOT NULL,
    sku VARCHAR(50) NOT NULL,
    supplier_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    promised_delivery_date DATE,
    actual_delivery_date DATE
);
CREATE TABLE logistics (
    shipment_id VARCHAR(50) PRIMARY KEY,
    purchase_order_id VARCHAR(50) NOT NULL,
    transport_mode VARCHAR(50),
    shipment_cost NUMERIC(15, 2),
    transit_delay_days INTEGER
);
SHOW TABLES;
SELECT * FROM calendar;
SELECT * FROM inventory; 
SELECT * FROM  logistics;
SELECT * FROM purchase_orders;
SELECT * FROM sales_forecast;
SELECT * FROM suppliers;
/*DIMENSION TABLES*/
CREATE TABLE dim_supplier AS
SELECT DISTINCT
    supplier_id,
    supplier_name,
    location,
    rating
FROM suppliers;
CREATE TABLE dim_product AS
SELECT DISTINCT
    sku AS product_id
FROM inventory;
CREATE TABLE dim_date AS
SELECT DISTINCT
    date,
    year,
    month,
    quarter
FROM calendar;
SELECT * FROM dim_supplier LIMIT 10;
SELECT * FROM dim_product LIMIT 10;
SELECT * FROM dim_date LIMIT 10;

/*FACT TABLES*/
CREATE TABLE fact_purchase_orders AS
SELECT
    purchase_order_id,
    supplier_id,
    sku AS product_id,
    order_date,
    promised_delivery_date,
    actual_delivery_date,
    quantity
FROM purchase_orders;

DROP TABLE IF EXISTS fact_inventory;
CREATE TABLE fact_inventory AS
SELECT
    sku AS product_id,
    warehouse_id,
    supplier_id,
    current_stock,
    reorder_level
FROM inventory;

DROP TABLE IF EXISTS fact_logistics;
CREATE TABLE fact_logistics AS
SELECT
    shipment_id,
    purchase_order_id,
    transport_mode,
    shipment_cost,
    transit_delay_days
FROM logistics;

DROP TABLE IF EXISTS fact_production;
CREATE TABLE fact_production AS
SELECT
    production_id,
    sku AS product_id,
    output_quantity,
    downtime_hours,
    downtime_reason
FROM production;
SELECT * FROM fact_purchase_orders LIMIT 10;
SELECT * FROM fact_inventory LIMIT 10;
SELECT * FROM fact_logistics LIMIT 10;
SELECT * FROM fact_production LIMIT 10;
SELECT COUNT(*) FROM dim_supplier;
SELECT COUNT(*) FROM fact_purchase_orders;

SELECT * FROM purchase_orders
WHERE actual_delivery_date IS NULL;

SELECT * FROM logistics
WHERE shipment_cost < 0;

SELECT purchase_order_id, COUNT(*)
FROM purchase_orders
GROUP BY purchase_order_id
HAVING COUNT(*) > 1;
/*KPI*/
/*DELIVERY DELAY*/
ALTER TABLE fact_purchase_orders
ADD COLUMN delivery_delay INT;
UPDATE fact_purchase_orders
SET delivery_delay = DATEDIFF(actual_delivery_date, promised_delivery_date);
SELECT purchase_order_id, delivery_delay
FROM fact_purchase_orders
LIMIT 10;
/*ON-TIME DELIVERY FLAG*/
ALTER TABLE fact_purchase_orders
ADD COLUMN on_time_flag INT;
UPDATE fact_purchase_orders
SET on_time_flag =
CASE 
    WHEN delivery_delay <= 0 THEN 1
    ELSE 0
END;
SELECT purchase_order_id, delivery_delay, on_time_flag
FROM fact_purchase_orders
LIMIT 10;

/*INVENTORY STATUS*/
ALTER TABLE fact_inventory
ADD COLUMN stock_status VARCHAR(20);
UPDATE fact_inventory
SET stock_status =
CASE 
    WHEN current_stock = 0 THEN 'Stockout'
    WHEN current_stock < reorder_level THEN 'Low Stock'
    ELSE 'Healthy'
END;
SELECT product_id, current_stock, reorder_level, stock_status
FROM fact_inventory
LIMIT 10;
/*LOGISTICS COST CATEGORY*/
ALTER TABLE fact_logistics
ADD COLUMN cost_category VARCHAR(20);
UPDATE fact_logistics
SET cost_category =
CASE 
    WHEN shipment_cost > 1000 THEN 'High'
    WHEN shipment_cost > 500 THEN 'Medium'
    ELSE 'Low'
END;
SELECT shipment_id, shipment_cost, cost_category
FROM fact_logistics
LIMIT 10;
/*PRODUCTION */
ALTER TABLE fact_production
ADD COLUMN downtime_flag VARCHAR(20);
UPDATE fact_production
SET downtime_flag =
CASE 
    WHEN downtime_hours > 5 THEN 'High'
    ELSE 'Normal'
END;
SELECT production_id, downtime_hours, downtime_flag
FROM fact_production
LIMIT 10;
/*LEAD TIME*/
ALTER TABLE fact_purchase_orders
ADD COLUMN lead_time INT;
UPDATE fact_purchase_orders
SET lead_time = DATEDIFF(actual_delivery_date, order_date);
/*ON-TIME DELIVERY % */
SELECT 
    ROUND(SUM(on_time_flag) * 100.0 / COUNT(*), 2) AS on_time_percentage
FROM fact_purchase_orders;
/*LOGISTICS COST %*/
ALTER TABLE fact_logistics
ADD COLUMN cost_percentage FLOAT;
UPDATE fact_logistics
SET cost_percentage = (shipment_cost / 10000) * 100;
/*DOWNTIME RATIO*/

ALTER TABLE fact_production
ADD COLUMN downtime_ratio FLOAT;

UPDATE fact_production
SET downtime_ratio = downtime_hours / NULLIF(output_quantity, 0);

SELECT production_id, output_quantity, downtime_hours, downtime_ratio
FROM fact_production
LIMIT 10;
SELECT 
    supplier_id,
    STDDEV(lead_time) AS lead_time_variability
FROM fact_purchase_orders
GROUP BY supplier_id;

# phase 3 --- Q1

WITH supplier_delay AS (
    SELECT
        f.supplier_id,
        d.supplier_name,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN delivery_delay > 0 THEN 1 ELSE 0 END) AS late_orders,
        AVG(delivery_delay) AS avg_delay
    FROM fact_purchase_orders f
    JOIN dim_supplier d
        ON f.supplier_id = d.supplier_id
    GROUP BY f.supplier_id, d.supplier_name
)

SELECT *,
       RANK() OVER (ORDER BY avg_delay DESC) AS delay_rank
FROM supplier_delay;

 #Q2: Calculate average delivery delay per supplier

SELECT
    f.supplier_id,                  -- Supplier ID
    d.supplier_name,                -- Supplier Name
    
    COUNT(*) AS total_orders,       -- Total number of orders
    
    AVG(f.delivery_delay) AS avg_delivery_delay   -- Average delay (in days)

FROM fact_purchase_orders f

JOIN dim_supplier d
ON f.supplier_id = d.supplier_id

GROUP BY f.supplier_id, d.supplier_name

ORDER BY avg_delivery_delay DESC;  -- Highest delay first

-- Q3: Month-over-Month Logistics Cost and Efficiency Analysis

SELECT
    DATE_FORMAT(po.order_date, '%Y-%m') AS month,   -- Extract year-month
    
    SUM(l.shipment_cost) AS total_logistics_cost,   -- Total cost per month
    
    AVG(l.transit_delay_days) AS avg_transit_delay  -- Average delay (efficiency)

FROM fact_logistics l

-- Join with purchase orders to get order date
JOIN fact_purchase_orders po
ON l.purchase_order_id = po.purchase_order_id

-- Group by month
GROUP BY DATE_FORMAT(po.order_date, '%Y-%m')

-- Sort by time (month)
ORDER BY month;

##Q4: Supplier Performance Ranking Based on Delivery

-- Step 1: Calculate supplier performance metrics
WITH supplier_metrics AS (
    SELECT
        supplier_id,
        
        COUNT(*) AS total_orders,   -- Total orders handled
        
        -- Count of on-time deliveries
        SUM(CASE 
            WHEN delivery_delay <= 0 THEN 1 
            ELSE 0 
        END) AS on_time_orders,
        
        -- Average delivery delay
        AVG(delivery_delay) AS avg_delay

    FROM fact_purchase_orders
    
    GROUP BY supplier_id
)
SELECT
    sm.supplier_id,
    d.supplier_name,
    
    -- On-time delivery percentage
    ROUND((sm.on_time_orders * 100.0 / sm.total_orders), 2) AS on_time_percentage,
    
    sm.avg_delay,
    
    -- Ranking logic: best supplier gets rank 1
    RANK() OVER (
        ORDER BY 
            (sm.on_time_orders * 1.0 / sm.total_orders) DESC,  -- Higher on-time % first
            sm.avg_delay ASC                                  -- Lower delay better
    ) AS supplier_rank

FROM supplier_metrics sm

JOIN dim_supplier d
ON sm.supplier_id = d.supplier_id

ORDER BY supplier_rank;

-- Q5: Identify Top 3 Delayed Suppliers per Month

-- Step 1: Calculate average delay per supplier per month
WITH monthly_supplier_delay AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m') AS month,   -- Extract month
        
        supplier_id,
        
        AVG(delivery_delay) AS avg_delay   -- Average delay per supplier per month
        
    FROM fact_purchase_orders
    
    GROUP BY DATE_FORMAT(order_date, '%Y-%m'), supplier_id
),
ranked_suppliers AS (
    SELECT
        msd.month,
        msd.supplier_id,
        d.supplier_name,
        msd.avg_delay,
        
        -- Assign rank per month (highest delay = rank 1)
        ROW_NUMBER() OVER (
            PARTITION BY msd.month
            ORDER BY msd.avg_delay DESC
        ) AS rank_per_month
        
    FROM monthly_supplier_delay msd
    
    JOIN dim_supplier d
    ON msd.supplier_id = d.supplier_id
)
SELECT *
FROM ranked_suppliers
WHERE rank_per_month <= 3
ORDER BY month, rank_per_month;

-- Q6: Calculate average shipment cost per transport mode

SELECT
    transport_mode,                          -- Mode of transport (Air, Road, etc.)
    
    COUNT(*) AS total_shipments,             -- Total shipments per mode
    
    AVG(shipment_cost) AS avg_shipment_cost  -- Average cost per transport mode

FROM fact_logistics

-- Group by transport mode to calculate per category
GROUP BY transport_mode

-- Sort by highest cost (most expensive mode first)
ORDER BY avg_shipment_cost DESC;