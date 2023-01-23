module SA2.Common.Table_rate_data


open System
open System.Linq
open Microsoft.FSharp.Data.TypeProviders


open DbSchema
open Dates
open Table_rate_header
open Utils



let get_rate_data ( descrs : string list )  =

    let sa2MarketDataContext = createSa2MarketDataContext ()    

    let rateId2Descr = get_rate_header descrs

    let rateIds = rateId2Descr |> keySet |> Set.toList

    query {

            for row in sa2MarketDataContext.Tbl_rate_data do 

            where ( rateIds.Contains row.Rate_Id )

            select ( row.Date , row.Rate_Id , row.Rate )

    }

    |> Seq.groupBy ( fun ( _ , i , _ ) -> i )
    |> Seq.map ( fun ( i , s ) -> ( i , s |> Seq.toList ) )
    |> Seq.toList
    |> List.map ( fun ( i , l ) -> ( Map.find i rateId2Descr , l |> List.filter ( fun ( _ , _ , v ) -> v.HasValue ) |> List.map ( fun ( d , _ , v ) -> ( d |> obj2Date , v.Value ) )) )
    |> Map.ofList
