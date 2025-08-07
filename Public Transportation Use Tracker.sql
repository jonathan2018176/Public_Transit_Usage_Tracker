-- =====================================================
-- PUBLIC TRANSPORTATION USE TRACKER - SQL DATABASE
-- =====================================================
-- Comprehensive system for managing 12,000+ trip records
-- from 500 users with real-time analytics capabilities
-- =====================================================

-- =====================================================
-- 1. DATABASE SCHEMA CREATION
-- =====================================================

-- Users table
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(15),
    registration_date DATE NOT NULL DEFAULT CURRENT_DATE,
    date_of_birth DATE,
    preferred_payment_method VARCHAR(20) DEFAULT 'card',
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'inactive'))
);

-- Transportation modes table
CREATE TABLE transportation_modes (
    mode_id SERIAL PRIMARY KEY,
    mode_name VARCHAR(30) NOT NULL UNIQUE,
    base_fare DECIMAL(5,2) NOT NULL,
    fare_per_mile DECIMAL(4,2),
    description TEXT
);

-- Routes table
CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    route_name VARCHAR(100) NOT NULL,
    mode_id INTEGER REFERENCES transportation_modes(mode_id),
    start_location VARCHAR(100) NOT NULL,
    end_location VARCHAR(100) NOT NULL,
    distance_miles DECIMAL(6,2),
    estimated_duration_minutes INTEGER,
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'maintenance', 'discontinued'))
);

-- Stations/Stops table
CREATE TABLE stations (
    station_id SERIAL PRIMARY KEY,
    station_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(20),
    zip_code VARCHAR(10)
);

-- Route stations junction table
CREATE TABLE route_stations (
    route_id INTEGER REFERENCES routes(route_id),
    station_id INTEGER REFERENCES stations(station_id),
    stop_order INTEGER NOT NULL,
    PRIMARY KEY (route_id, station_id)
);

-- Main trips table
CREATE TABLE trips (
    trip_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    route_id INTEGER NOT NULL REFERENCES routes(route_id),
    start_station_id INTEGER REFERENCES stations(station_id),
    end_station_id INTEGER REFERENCES stations(station_id),
    trip_date DATE NOT NULL DEFAULT CURRENT_DATE,
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    fare_paid DECIMAL(6,2) NOT NULL,
    payment_method VARCHAR(20) DEFAULT 'card',
    trip_status VARCHAR(20) DEFAULT 'completed' CHECK (trip_status IN ('completed', 'cancelled', 'in_progress')),
    distance_traveled DECIMAL(6,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User monthly summaries (for performance optimization)
CREATE TABLE user_monthly_summaries (
    summary_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    total_trips INTEGER DEFAULT 0,
    total_spent DECIMAL(8,2) DEFAULT 0.00,
    total_distance DECIMAL(8,2) DEFAULT 0.00,
    most_used_mode VARCHAR(30),
    avg_trip_cost DECIMAL(6,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, year, month)
);

-- Route analytics summary table
CREATE TABLE route_analytics (
    route_id INTEGER PRIMARY KEY REFERENCES routes(route_id),
    total_trips INTEGER DEFAULT 0,
    total_revenue DECIMAL(10,2) DEFAULT 0.00,
    avg_trips_per_day DECIMAL(6,2),
    peak_usage_hour INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 2. INDEXES FOR PERFORMANCE OPTIMIZATION
-- =====================================================

-- Primary performance indexes
CREATE INDEX idx_trips_user_date ON trips(user_id, trip_date);
CREATE INDEX idx_trips_route_date ON trips(route_id, trip_date);
CREATE INDEX idx_trips_date ON trips(trip_date);
CREATE INDEX idx_trips_start_time ON trips(start_time);
CREATE INDEX idx_user_monthly_summaries_user_period ON user_monthly_summaries(user_id, year, month);

-- Geographic indexes for location-based queries
CREATE INDEX idx_stations_location ON stations(latitude, longitude);
CREATE INDEX idx_routes_mode ON routes(mode_id);

-- Composite indexes for common query patterns
CREATE INDEX idx_trips_user_mode_date ON trips(user_id, trip_date) 
    INCLUDE (route_id, fare_paid);
CREATE INDEX idx_trips_payment_method ON trips(payment_method, trip_date);

-- =====================================================
-- 3. SEQUENCES AND TRIGGERS
-- =====================================================

-- Sequence for trip numbering
CREATE SEQUENCE trip_number_seq START 1000000;

-- Function to update monthly summaries
CREATE OR REPLACE FUNCTION update_monthly_summary()
RETURNS TRIGGER AS $$
BEGIN
    -- Insert or update monthly summary
    INSERT INTO user_monthly_summaries (user_id, year, month, total_trips, total_spent, total_distance)
    VALUES (
        NEW.user_id,
        EXTRACT(YEAR FROM NEW.trip_date),
        EXTRACT(MONTH FROM NEW.trip_date),
        1,
        NEW.fare_paid,
        COALESCE(NEW.distance_traveled, 0)
    )
    ON CONFLICT (user_id, year, month)
    DO UPDATE SET
        total_trips = user_monthly_summaries.total_trips + 1,
        total_spent = user_monthly_summaries.total_spent + NEW.fare_paid,
        total_distance = user_monthly_summaries.total_distance + COALESCE(NEW.distance_traveled, 0),
        last_updated = CURRENT_TIMESTAMP;

    -- Update route analytics
    INSERT INTO route_analytics (route_id, total_trips, total_revenue)
    VALUES (NEW.route_id, 1, NEW.fare_paid)
    ON CONFLICT (route_id)
    DO UPDATE SET
        total_trips = route_analytics.total_trips + 1,
        total_revenue = route_analytics.total_revenue + NEW.fare_paid,
        last_updated = CURRENT_TIMESTAMP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update summaries
CREATE TRIGGER trigger_update_monthly_summary
    AFTER INSERT ON trips
    FOR EACH ROW
    EXECUTE FUNCTION update_monthly_summary();

-- Function to calculate average trip costs
CREATE OR REPLACE FUNCTION update_avg_costs()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE user_monthly_summaries 
    SET avg_trip_cost = CASE 
        WHEN total_trips > 0 THEN total_spent / total_trips 
        ELSE 0 
    END
    WHERE user_id = NEW.user_id 
    AND year = EXTRACT(YEAR FROM NEW.trip_date)
    AND month = EXTRACT(MONTH FROM NEW.trip_date);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_avg_costs
    AFTER INSERT ON trips
    FOR EACH ROW
    EXECUTE FUNCTION update_avg_costs();

-- =====================================================
-- 4. SAMPLE DATA INSERTION
-- =====================================================

-- Insert transportation modes
INSERT INTO transportation_modes (mode_name, base_fare, fare_per_mile, description) VALUES
('Bus', 2.50, 0.15, 'City bus service with multiple routes'),
('Subway', 3.00, 0.20, 'Underground rail system'),
('Light Rail', 3.50, 0.25, 'Above-ground electric rail'),
('Ferry', 5.00, 0.30, 'Water transportation service'),
('Commuter Rail', 4.00, 0.35, 'Regional train service');

-- Insert sample stations
INSERT INTO stations (station_name, latitude, longitude, city, state) VALUES
('Downtown Central', 42.3601, -71.0589, 'Boston', 'MA'),
('North Station', 42.3665, -71.0615, 'Boston', 'MA'),
('South Station', 42.3519, -71.0552, 'Boston', 'MA'),
('Back Bay', 42.3487, -71.0753, 'Boston', 'MA'),
('Harvard Square', 42.3736, -71.1190, 'Cambridge', 'MA'),
('MIT', 42.3596, -71.0935, 'Cambridge', 'MA'),
('Airport Terminal', 42.3656, -71.0096, 'Boston', 'MA'),
('Fenway', 42.3467, -71.0972, 'Boston', 'MA');

-- Insert sample routes
INSERT INTO routes (route_name, mode_id, start_location, end_location, distance_miles, estimated_duration_minutes) VALUES
('Red Line North', 2, 'South Station', 'Harvard Square', 8.5, 25),
('Blue Line', 2, 'Downtown Central', 'Airport Terminal', 6.2, 18),
('Green Line B', 3, 'Downtown Central', 'Boston College', 12.3, 35),
('Bus Route 1', 1, 'Harvard Square', 'MIT', 2.1, 12),
('Commuter Rail North', 5, 'North Station', 'Lowell', 25.4, 45),
('Ferry Service', 4, 'Downtown Central', 'Logan Airport', 3.8, 20);

-- Insert sample users (500 users)
INSERT INTO users (username, email, phone, registration_date, preferred_payment_method)
SELECT 
    'user_' || generate_series,
    'user' || generate_series || '@email.com',
    '555-' || LPAD(generate_series::text, 4, '0') || '00',
    CURRENT_DATE - (random() * 180)::integer,
    CASE (random() * 3)::integer 
        WHEN 0 THEN 'card'
        WHEN 1 THEN 'mobile'
        ELSE 'cash'
    END
FROM generate_series(1, 500);

-- Generate 12,000+ trip records over 6 months
INSERT INTO trips (user_id, route_id, start_station_id, end_station_id, trip_date, start_time, fare_paid, payment_method, distance_traveled)
SELECT 
    (random() * 499 + 1)::integer as user_id,
    (random() * 5 + 1)::integer as route_id,
    (random() * 7 + 1)::integer as start_station_id,
    (random() * 7 + 1)::integer as end_station_id,
    CURRENT_DATE - (random() * 180)::integer as trip_date,
    CURRENT_TIMESTAMP - (random() * 180)::integer * interval '1 day' - (random() * 86400)::integer * interval '1 second',
    (random() * 8 + 2)::numeric(5,2) as fare_paid,
    CASE (random() * 3)::integer 
        WHEN 0 THEN 'card'
        WHEN 1 THEN 'mobile'
        ELSE 'cash'
    END,
    (random() * 20 + 1)::numeric(6,2) as distance_traveled
FROM generate_series(1, 12000)
WHERE (random() * 7 + 1)::integer <= 8  -- Ensure station IDs are valid
AND (random() * 7 + 1)::integer != (random() * 7 + 1)::integer; -- Ensure start != end

-- Alternative safer approach: Generate trips with explicit station validation
DELETE FROM trips; -- Clear any partial data

-- Generate trips with proper foreign key validation
WITH valid_combinations AS (
    SELECT 
        u.user_id,
        r.route_id,
        s1.station_id as start_station_id,
        s2.station_id as end_station_id,
        tm.base_fare + (random() * tm.fare_per_mile * r.distance_miles) as calculated_fare
    FROM users u
    CROSS JOIN routes r
    CROSS JOIN stations s1
    CROSS JOIN stations s2
    JOIN transportation_modes tm ON r.mode_id = tm.mode_id
    WHERE s1.station_id != s2.station_id
),
trip_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY random()) as rn
    FROM valid_combinations
)
INSERT INTO trips (user_id, route_id, start_station_id, end_station_id, trip_date, start_time, fare_paid, payment_method, distance_traveled)
SELECT 
    user_id,
    route_id,
    start_station_id,
    end_station_id,
    CURRENT_DATE - (random() * 180)::integer as trip_date,
    CURRENT_TIMESTAMP - (random() * 180)::integer * interval '1 day' - (random() * 86400)::integer * interval '1 second',
    LEAST(calculated_fare::numeric(6,2), 15.00) as fare_paid, -- Cap at reasonable fare
    CASE (random() * 3)::integer 
        WHEN 0 THEN 'card'
        WHEN 1 THEN 'mobile'
        ELSE 'cash'
    END,
    (random() * 20 + 1)::numeric(6,2) as distance_traveled
FROM trip_data
WHERE rn <= 12000;

-- =====================================================
-- 5. ANALYTICAL QUERIES FOR TABLEAU
-- =====================================================

-- Query 1: Monthly spending summary by user
CREATE OR REPLACE VIEW v_monthly_user_spending AS
SELECT 
    u.user_id,
    u.username,
    ums.year,
    ums.month,
    ums.total_trips,
    ums.total_spent,
    ums.total_distance,
    ums.avg_trip_cost,
    ums.most_used_mode
FROM user_monthly_summaries ums
JOIN users u ON ums.user_id = u.user_id
ORDER BY ums.year DESC, ums.month DESC, ums.total_spent DESC;

-- Query 2: Transportation mode comparison
CREATE OR REPLACE VIEW v_mode_comparison AS
SELECT 
    tm.mode_name,
    COUNT(t.trip_id) as total_trips,
    SUM(t.fare_paid) as total_revenue,
    AVG(t.fare_paid) as avg_fare,
    AVG(t.distance_traveled) as avg_distance,
    COUNT(DISTINCT t.user_id) as unique_users,
    MAX(t.trip_date) as last_trip_date
FROM transportation_modes tm
JOIN routes r ON tm.mode_id = r.mode_id
JOIN trips t ON r.route_id = t.route_id
GROUP BY tm.mode_id, tm.mode_name
ORDER BY total_trips DESC;

-- Query 3: Route optimization analysis
CREATE OR REPLACE VIEW v_route_optimization AS
SELECT 
    r.route_name,
    tm.mode_name,
    ra.total_trips,
    ra.total_revenue,
    ra.avg_trips_per_day,
    r.distance_miles,
    (ra.total_revenue / NULLIF(ra.total_trips, 0)) as avg_revenue_per_trip,
    (ra.total_revenue / NULLIF(r.distance_miles, 0)) as revenue_per_mile,
    CASE 
        WHEN ra.total_trips > 1000 THEN 'High Usage'
        WHEN ra.total_trips > 500 THEN 'Medium Usage'
        ELSE 'Low Usage'
    END as usage_category
FROM routes r
JOIN transportation_modes tm ON r.mode_id = tm.mode_id
LEFT JOIN route_analytics ra ON r.route_id = ra.route_id
ORDER BY ra.total_revenue DESC NULLS LAST;

-- Query 4: Peak usage analysis by time
CREATE OR REPLACE VIEW v_peak_usage_analysis AS
SELECT 
    EXTRACT(HOUR FROM start_time) as hour_of_day,
    EXTRACT(DOW FROM trip_date) as day_of_week,
    COUNT(*) as trip_count,
    AVG(fare_paid) as avg_fare,
    SUM(fare_paid) as total_revenue,
    CASE EXTRACT(DOW FROM trip_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name
FROM trips
WHERE trip_date >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY EXTRACT(HOUR FROM start_time), EXTRACT(DOW FROM trip_date)
ORDER BY trip_count DESC;

-- Query 5: User behavior segmentation
CREATE OR REPLACE VIEW v_user_segmentation AS
SELECT 
    u.user_id,
    u.username,
    u.preferred_payment_method,
    COUNT(t.trip_id) as total_trips,
    SUM(t.fare_paid) as total_spent,
    AVG(t.fare_paid) as avg_trip_cost,
    COUNT(DISTINCT r.mode_id) as modes_used,
    COUNT(DISTINCT DATE_TRUNC('month', t.trip_date)) as active_months,
    CASE 
        WHEN COUNT(t.trip_id) > 100 THEN 'Heavy User'
        WHEN COUNT(t.trip_id) > 50 THEN 'Regular User'
        WHEN COUNT(t.trip_id) > 10 THEN 'Occasional User'
        ELSE 'Light User'
    END as user_segment
FROM users u
LEFT JOIN trips t ON u.user_id = t.user_id
LEFT JOIN routes r ON t.route_id = r.route_id
GROUP BY u.user_id, u.username, u.preferred_payment_method
ORDER BY total_trips DESC;

-- Query 6: Revenue trends over time
CREATE OR REPLACE VIEW v_revenue_trends AS
SELECT 
    DATE_TRUNC('month', trip_date) as month,
    COUNT(*) as total_trips,
    SUM(fare_paid) as total_revenue,
    AVG(fare_paid) as avg_fare,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(distance_traveled) as total_distance
FROM trips
WHERE trip_date >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY DATE_TRUNC('month', trip_date)
ORDER BY month;

-- =====================================================
-- 6. PERFORMANCE OPTIMIZATION QUERIES
-- =====================================================

-- Efficient query for real-time dashboard (optimized with indexes)
CREATE OR REPLACE FUNCTION get_user_dashboard_data(p_user_id INTEGER)
RETURNS TABLE (
    current_month_trips INTEGER,
    current_month_spending DECIMAL(8,2),
    favorite_route VARCHAR(100),
    total_lifetime_trips INTEGER,
    total_lifetime_spending DECIMAL(10,2)
) AS $$
BEGIN
    RETURN QUERY
    WITH current_month AS (
        SELECT 
            COALESCE(total_trips, 0) as trips,
            COALESCE(total_spent, 0.00) as spending
        FROM user_monthly_summaries 
        WHERE user_id = p_user_id 
        AND year = EXTRACT(YEAR FROM CURRENT_DATE)
        AND month = EXTRACT(MONTH FROM CURRENT_DATE)
    ),
    favorite_route AS (
        SELECT r.route_name
        FROM trips t
        JOIN routes r ON t.route_id = r.route_id
        WHERE t.user_id = p_user_id
        GROUP BY r.route_id, r.route_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ),
    lifetime_stats AS (
        SELECT 
            COUNT(*)::INTEGER as total_trips,
            SUM(fare_paid)::DECIMAL(10,2) as total_spending
        FROM trips
        WHERE user_id = p_user_id
    )
    SELECT 
        cm.trips,
        cm.spending,
        fr.route_name,
        ls.total_trips,
        ls.total_spending
    FROM current_month cm
    CROSS JOIN favorite_route fr
    CROSS JOIN lifetime_stats ls;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 7. DATA QUALITY AND MAINTENANCE
-- =====================================================

-- Function to clean old data (optional maintenance)
CREATE OR REPLACE FUNCTION cleanup_old_data(months_to_keep INTEGER DEFAULT 12)
RETURNS INTEGER AS $$
DECLARE
    rows_deleted INTEGER;
BEGIN
    DELETE FROM trips 
    WHERE trip_date < CURRENT_DATE - (months_to_keep || ' months')::INTERVAL;
    
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    
    -- Clean up orphaned summary records
    DELETE FROM user_monthly_summaries 
    WHERE (year, month) < (EXTRACT(YEAR FROM CURRENT_DATE - (months_to_keep || ' months')::INTERVAL),
                          EXTRACT(MONTH FROM CURRENT_DATE - (months_to_keep || ' months')::INTERVAL));
    
    RETURN rows_deleted;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 8. TABLEAU READY VIEWS AND PROCEDURES
-- =====================================================

-- Master view for Tableau dashboard
CREATE OR REPLACE VIEW v_tableau_master_data AS
SELECT 
    t.trip_id,
    t.user_id,
    u.username,
    u.preferred_payment_method,
    r.route_name,
    tm.mode_name as transportation_mode,
    s1.station_name as start_station,
    s2.station_name as end_station,
    s1.city as start_city,
    s2.city as end_city,
    t.trip_date,
    EXTRACT(YEAR FROM t.trip_date) as year,
    EXTRACT(MONTH FROM t.trip_date) as month,
    EXTRACT(DOW FROM t.trip_date) as day_of_week,
    EXTRACT(HOUR FROM t.start_time) as hour,
    t.fare_paid,
    t.distance_traveled,
    t.payment_method,
    r.distance_miles as route_distance,
    r.estimated_duration_minutes,
    CASE 
        WHEN EXTRACT(DOW FROM t.trip_date) IN (0,6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    CASE 
        WHEN EXTRACT(HOUR FROM t.start_time) BETWEEN 7 AND 9 THEN 'Morning Rush'
        WHEN EXTRACT(HOUR FROM t.start_time) BETWEEN 17 AND 19 THEN 'Evening Rush'
        ELSE 'Off Peak'
    END as time_period
FROM trips t
JOIN users u ON t.user_id = u.user_id
JOIN routes r ON t.route_id = r.route_id
JOIN transportation_modes tm ON r.mode_id = tm.mode_id
LEFT JOIN stations s1 ON t.start_station_id = s1.station_id
LEFT JOIN stations s2 ON t.end_station_id = s2.station_id;

-- Summary statistics for Tableau KPIs
CREATE OR REPLACE VIEW v_tableau_kpi_summary AS
SELECT 
    COUNT(DISTINCT user_id) as total_users,
    COUNT(*) as total_trips,
    SUM(fare_paid) as total_revenue,
    AVG(fare_paid) as avg_fare,
    COUNT(DISTINCT route_name) as active_routes,
    COUNT(DISTINCT transportation_mode) as active_modes,
    MIN(trip_date) as data_start_date,
    MAX(trip_date) as data_end_date,
    SUM(distance_traveled) as total_distance,
    AVG(distance_traveled) as avg_distance
FROM v_tableau_master_data;

-- =====================================================
-- 9. TABLEAU CONNECTION INSTRUCTIONS
-- =====================================================

/*
TABLEAU CONNECTION SETUP:

1. Connect to PostgreSQL database using:
   - Server: [your_server]
   - Database: [your_database_name]
   - Authentication: [your_credentials]

2. Primary tables/views to use:
   - v_tableau_master_data (main dataset)
   - v_monthly_user_spending (user spending analysis)
   - v_mode_comparison (transportation mode analysis)
   - v_route_optimization (route performance)
   - v_peak_usage_analysis (time-based analysis)
   - v_user_segmentation (user behavior)
   - v_revenue_trends (trend analysis)

3. Recommended Tableau visualizations:
   - Monthly spending trends (line chart)
   - Mode comparison (bar chart)
   - Route optimization heatmap
   - Peak usage by hour/day (heatmap)
   - User segmentation pie chart
   - Geographic route map (if coordinates available)

4. Key metrics to display:
   - Total revenue, trips, users
   - Average fare per trip
   - Most popular routes/modes
   - Peak usage times
   - User retention rates

5. Filters to implement:
   - Date range picker
   - Transportation mode filter
   - User segment filter
   - Route filter
   - Payment method filter
*/

-- =====================================================
-- END OF PUBLIC TRANSPORTATION TRACKER DATABASE
-- =====================================================