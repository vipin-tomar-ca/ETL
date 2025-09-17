--@name: SelectProductsWithFullTextSearch
--@description: SQL Server query using full-text search capabilities
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: products,select,full-text-search,sqlserver

SELECT 
    p.ProductID,
    p.ProductName,
    p.Description,
    p.Price,
    p.CategoryID,
    c.CategoryName,
    p.UnitsInStock,
    p.UnitsOnOrder,
    p.ReorderLevel,
    p.Discontinued,
    p.CreatedDate,
    p.ModifiedDate,
    KEY_TBL.RANK as SearchRank
FROM Products p
INNER JOIN Categories c ON p.CategoryID = c.CategoryID
INNER JOIN CONTAINSTABLE(Products, (ProductName, Description), @SearchTerm) AS KEY_TBL
    ON p.ProductID = KEY_TBL.[KEY]
WHERE p.Discontinued = 0
    AND p.UnitsInStock > 0
ORDER BY KEY_TBL.RANK DESC, p.ProductName;
