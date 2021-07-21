CREATE TABLE IF NOT EXISTS STOCKPRICE (
    id UUID default random_uuid() PRIMARY KEY ,
    symbol varchar(5),
    price DECIMAL(5,2),
    date DATE
);