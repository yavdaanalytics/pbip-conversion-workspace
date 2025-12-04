CREATE TABLE [dbo].[FactSalesOrder] (

	[SalesOrderKey] int NOT NULL, 
	[SalesOrderDateKey] int NOT NULL, 
	[ProductKey] int NOT NULL, 
	[CustomerKey] int NOT NULL, 
	[Quantity] int NULL, 
	[SalesTotal] decimal(18,0) NULL
);


GO
ALTER TABLE [dbo].[FactSalesOrder] ADD CONSTRAINT FK_434154d2_8443_45c8_bd8d_332beffbb894 FOREIGN KEY ([SalesOrderDateKey]) REFERENCES [dbo].[DimDate]([DateKey]);
GO
ALTER TABLE [dbo].[FactSalesOrder] ADD CONSTRAINT FK_460582c8_c4a2_442c_b0c6_b454f9c1a81e FOREIGN KEY ([CustomerKey]) REFERENCES [dbo].[DimCustomer]([CustomerKey]);
GO
ALTER TABLE [dbo].[FactSalesOrder] ADD CONSTRAINT FK_808f85dd_1913_476a_8e28_e1f0e1cb36a7 FOREIGN KEY ([ProductKey]) REFERENCES [dbo].[DimProduct]([ProductKey]);