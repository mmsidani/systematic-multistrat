module SA2.Common.Table_rate_header


open System
open System.Linq
open Microsoft.FSharp.Data.TypeProviders


open DbSchema



let get_rate_header ( descrs : string list ) =

    let sa2MarketDataContext = createSa2MarketDataContext ()

    query {

        for row in sa2MarketDataContext.Tbl_rate_header do

        where ( descrs.Contains row.Descr )

        select (  row.Rate_Id , row.Descr )

    }

    |> Map.ofSeq