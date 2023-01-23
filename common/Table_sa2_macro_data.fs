module SA2.Common.Table_sa2_macro_data

open System
open System.Linq
open Microsoft.FSharp.Data.TypeProviders


open DbSchema
open Dates

open Table_rate_header
open Utils


let get_sa2_macro_data ( descrs : string list )  =

    let sa2MarketDataContext = createSa2MarketDataContext ()

    let rateId2Descr = get_rate_header descrs

    let rateIds = rateId2Descr |> keySet |> Set.toList

    query 

        {

            for row in sa2MarketDataContext.Tbl_sa2_macro_data do 

            where ( rateIds.Contains row.Rate_Id )

            select ( row.Date , row.Date_Entered , row.Rate_Id , row.Rate )

        }

        |> Seq.groupBy ( fun ( _ , _ , i , _ ) -> i )
        |> Seq.map ( fun ( i , ss ) -> 
                        ( Map.find i rateId2Descr , 

                            ss 

                            |> Seq.groupBy ( fun ( d , _ , _ , _ ) -> d ) 
                            |> Seq.map ( fun ( _ , s ) -> 
                                            s 
                                            |> Seq.toList 
                                            |> List.filter ( fun ( _ , _ , _ , v ) -> v.HasValue ) 
                                            |> List.sortBy ( fun ( _ , d , _ , _ ) -> d ) 
                                            |> List.rev
                                            |> List.head 
                                            |> ( fun ( d , _ , _ , v ) -> ( d |> obj2Date , v.Value ) ) ) 

                            |> Seq.toList ) )
        |> Seq.toList
        |> Map.ofList