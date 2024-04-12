package constants

object SchedulerConstants {

  val ALL_TABLES: Array[String] = Array("SQL-DSSI-DEV-TRAN.CONTACT.dbo.Incorp","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spPriceLevelDefaults","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spPriceLevelFacility","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.scSupplier","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spPrice","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spProduct","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spProductUM","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spProductUMHist","SQL-DSSI-DEV-TRAN.PRODUCT.dbo.Inprmain","SQL-DSSI-DEV-TRAN.PRODUCT.dbo.Inprcost","SQL-DSSI-DEV-TRAN.PRODUCT.dbo.Inprperm","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcProvider","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcSupplier","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcCategory","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcFeature","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcBitFeature","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcSetting","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcBitSetting","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcProviderSupplier")

  // This supports running table groups instead of running all tables
  // Not being used right now
  val CONTACT_TABLES: Array[String] = Array("SQL-DSSI-DEV-TRAN.CONTACT.dbo.Incorp")
  val SUPPLIER_TABLES: Array[String] = Array("SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spPriceLevelDefaults","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spPriceLevelFacility","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.scSupplier","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spPrice","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spProduct","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spProductUM","SQL-DSSI-DEV-TRAN.SUPPLIER.dbo.spProductUMHist")
  val PRODUCT_TABLES: Array[String] = Array("SQL-DSSI-DEV-TRAN.PRODUCT.dbo.Inprmain","SQL-DSSI-DEV-TRAN.PRODUCT.dbo.Inprcost")
  val PRODUCT_LARGE_TABLE: Array[String] = Array("SQL-DSSI-DEV-TRAN.PRODUCT.dbo.Inprperm")
  val SUPPLIER_HIST_TABLES: Array[String] = Array("SQL-DSSI-DI-DW.SUPPLIER_HIST.dbo.spProductHist")
  val TRANSPROCCONFIG_TABLES: Array[String] = Array("SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcProvider","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcSupplier","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcCategory","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcFeature","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcBitFeature","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcSetting","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcBitSetting","SQL-DSSI-DEV-TRAN.TRANSPROCCONFIG.dbo.tpcProviderSupplier")

  val TABLE_GROUPS: Array[Array[String]] = Array(ALL_TABLES, SUPPLIER_HIST_TABLES/*CONTACT_TABLES, SUPPLIER_TABLES, PRODUCT_TABLES, PRODUCT_LARGE_TABLE, SUPPLIER_HIST_TABLES, TRANSPROCCONFIG_TABLES*/)
  val TABLE_GROUP_NAMES: Array[String] = Array("Tran Tables", "DW Tables"/*"Contact", "Supplier (Tran)", "Product", "Product (Large Table)", "Supplier Hist", "TransProcConfig"*/)

}
