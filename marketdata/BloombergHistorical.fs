module SA2.MarketData.BloombergHistorical

open System

open Bloomberglp.Blpapi

open SA2.Common.Dates

open BloombergCore

let private bloombergSendRequest ( session : Session ) ( names : string list ) ( field : string ) ( frequency : string ) fillNonTradingDays ( startDate : string ) ( endDate : string ) =

    try

        let refDataService = session.GetService( "//blp/refdata" )
        let request = refDataService.CreateRequest( "HistoricalDataRequest" )
            
        let securities = request.GetElement( "securities" )
        names |> List.iter ( fun n -> securities.AppendValue( n ) )

        let fieldsObj = request.GetElement( "fields" )
        fieldsObj.AppendValue( field )

        request.Set( "periodicityAdjustment", "CALENDAR" )
        request.Set( "periodicitySelection", frequency.ToUpper() )
        request.Set( "startDate", startDate)
        request.Set( "endDate", endDate)

        if fillNonTradingDays then

            request.Set( "nonTradingDayFillOption", "NON_TRADING_WEEKDAYS" )
            request.Set( "nonTradingDayFillMethod", "PREVIOUS_VALUE" )

        session.SendRequest( request, null )

    with

    | _ as e -> raise( e )




let bloombergProcessHistoricalFields ( field : string ) ( msg : Message ) =

    let securityData = msg.GetElement( "securityData" )
    let securityName = securityData.GetElementAsString( "security" )
    let fieldData = securityData.GetElement( "fieldData" )
   
    let mutable ret = Array.empty

    if fieldData.NumValues > 0 then

        for j = 0 to fieldData.NumValues - 1 do

            let element = fieldData.GetValueAsElement( j )
       
            let date = element.GetElementAsDatetime( "date" ).ToString() |> bbDateTime2ymd |> ( fun ( y , m , d ) -> int ( y + m + d ) )
        
            if element.HasElement( field ) then

                // ( date, securityName, field) . Note: so convert all types of field to string and then let the calling function convert them to the appropriate type
                ret <- Array.append ret [| ( date , securityName , element.GetElementAsString( field ) ) |]

    ret




let bloombergHistoricalRequest names field frequency fillNonTradingDays startDate endDate  =

    // one field per request and then we don't have to worry about mapping field values to different suffixes in security names

    if List.length names <> 0 then

        try

            match bloombergSession() 
            
                with

                    | Some( session ) -> 
            
                        bloombergSendRequest session names field frequency fillNonTradingDays startDate endDate |> ignore

                        let ret = bloombergProcessHistoricalFields field |> bloombergEventLoop session

                        session.Stop ()

                        ret

                    | None -> 

                        raise ( Exception( "bloomberg session failed to start" ) )
        
        with

            | _ as e -> raise( e )

    else 

        [||]

