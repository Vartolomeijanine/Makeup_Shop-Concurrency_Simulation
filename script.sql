CREATE TABLE Categories (
    CategoryID INT PRIMARY KEY,
    CategoryName VARCHAR(255) NOT NULL
);

CREATE TABLE Suppliers (
    SupplierID INT PRIMARY KEY,
    SupplierName VARCHAR(255) NOT NULL
);

CREATE TABLE Makeup_Products (
    ProductID INT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Brand VARCHAR(255) NOT NULL,
    Price DECIMAL(10,2) NOT NULL
);
