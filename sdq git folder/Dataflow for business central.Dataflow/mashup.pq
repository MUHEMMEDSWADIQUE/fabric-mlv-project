[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "customers_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared customers = let
  Source = Dynamics365BusinessCentral.ApiContentsWithOptions(null, "CRONUS IN", null, [UseReadOnlyReplica = null, AcceptLanguage = null, Timeout = null, ODataMaxPageSize = null]),
  #"Navigation 1" = Source{[Name = "PRODUCTION"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "CRONUS IN"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "v2.0"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "customers", Signature = "table"]}[Data]
in
  #"Navigation 4";
shared customers_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "7563807f-8ec1-465e-a994-c3ba0ab570b7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8e62d8bd-0021-4972-a8fb-a4f09d943d96"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "customers", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
