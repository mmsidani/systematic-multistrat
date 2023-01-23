module SA2.MarketData.BloombergReference

open System

open Bloomberglp.Blpapi

open SA2.Common.Dates
open BloombergCore



let private bloombergSendRequest ( session : Session ) ( names : string list ) ( field : string ) ( forDate : int )   =

    try

        let refDataService = session.GetService( "//blp/refdata" )
        let request = refDataService.CreateRequest( "ReferenceDataRequest" )

        let securities = request.GetElement( "securities" )
        names |> List.iter ( fun n -> securities.AppendValue( n ) )

        let fieldsObj = request.GetElement( "fields" )
        fieldsObj.AppendValue( field )

        let allOverrides = request.GetElement( "overrides" )
        let dateOverride = allOverrides.AppendElement()
        dateOverride.SetElement( "fieldId", "END_DATE_OVERRIDE" )
        dateOverride.SetElement( "value", forDate )

        session.SendRequest( request, null )

    with

    | _ as e -> raise( e )




let bloombergProcessReferenceFields ( field : string ) ( forDate : int ) ( msg : Message ) =

    let securityData = msg.GetElement( "securityData" )

    let mutable ret = Array.empty

    for i in 0 .. securityData.NumValues - 1 do
        
        let oneSecurityData = securityData.GetValueAsElement( i )

        let securityName = oneSecurityData.GetElementAsString( "security" )
        let fieldData = oneSecurityData.GetElement( "fieldData" )
        
    
        if fieldData.NumValues > 0 then
            
            let ( d , m , y ) = date2dmy forDate
            let date = int ( y + m + d )

            for j = 0 to fieldData.NumElements - 1 do
                
                let element = fieldData.GetElement( j )
                
                if element.Datatype <> Schema.Datatype.SEQUENCE then

                    ret <- Array.append ret [| ( date , securityName , element.GetValueAsString() ) |]

                elif element.Datatype = Schema.Datatype.SEQUENCE then

                    raise( Exception( "did NOT expect elements of type Sequence" ) )                 

    ret




let bloombergReferenceRequest names field date  =

    // one field per request and then we don't have to worry about mapping field values to different suffixes in security names

    if List.length names <> 0 then

        try

            match bloombergSession() 
            
                with

                    | Some( session ) -> 
                        
                        bloombergSendRequest session names field date |> ignore

                        let ret = bloombergProcessReferenceFields field date |> bloombergEventLoop session 

                        session.Stop ()

                        ret

                    | None -> 

                        raise ( Exception( "bloomberg session failed to start" ) )
        
        with

        | _ as e -> raise( e )
        
    else 

        [||]





let bloombergProcessIndexMembersFields ( forDate : int ) ( msg : Message ) =

    let securityData = msg.GetElement( "securityData" )

    let mutable ret = Array.empty

    for i in 0 .. securityData.NumValues - 1 do
        
        let oneSecurityData = securityData.GetValueAsElement( i )

        let securityName = oneSecurityData.GetElementAsString( "security" )
        let fieldData = oneSecurityData.GetElement( "fieldData" )
        
    
        if fieldData.NumValues > 0 then
            
            let ( d , m , y ) = date2dmy forDate
            let date = int ( y + m + d )

            for j = 0 to fieldData.NumElements - 1 do
                
                let element = fieldData.GetElement( j )
                
                if element.Datatype = Schema.Datatype.SEQUENCE then

                    for l in 0 .. element.NumValues - 1 do
                        
                        let lField = element.GetValueAsElement( l )
    
                        let mutable indexMemberName = ""
                        let mutable indexMemberWeight = 0.0

                        for f in 0 .. lField.NumElements - 1 do
                            
                            let fMember = lField.GetElement(f)

                            if fMember.Datatype = Schema.Datatype.STRING then
                                indexMemberName <- fMember.GetValueAsString ()
                            elif fMember.Datatype = Schema.Datatype.FLOAT64 then
                                indexMemberWeight <- fMember.GetValueAsFloat64 ()

                        ret <- Array.append ret [| ( date , securityName , indexMemberName , if indexMemberWeight < 0.0 then -1.0 else indexMemberWeight / 100.0 ) |]

                else

                    raise( Exception( "expected elements of type Sequence to be returned" ) )

    ret




let bloombergIndexMembersReferenceRequest names date  =

    if List.length names <> 0 then

        try

            match bloombergSession() 
            
                with

                    | Some( session ) -> 
                        
                        bloombergSendRequest session names "INDX_MWEIGHT_HIST" date |> ignore

                        let ret = bloombergProcessIndexMembersFields date |> bloombergEventLoop session 

                        session.Stop ()

                        ret

                    | None -> 

                        raise ( Exception( "bloomberg session failed to start" ) )
        
        with

        | _ as e -> raise( e )
        
    else 

        [||]