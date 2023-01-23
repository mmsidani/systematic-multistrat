module SA2.MarketData.BloombergCore

open System

open Bloomberglp.Blpapi




let bloombergSession () =

    // infrastructure stuff. taken from the bb examples. hard-coded stuff, yes, but doesn't matter. doesn't change

    let serverHost = "localhost"
    let serverPort = 8194

    let sessionOptions = new SessionOptions()
    sessionOptions.ServerHost <- serverHost
    sessionOptions.ServerPort <- serverPort

    let session = new Session( sessionOptions )
        
    try

       let sessionStarted = session.Start()

       if sessionStarted && session.OpenService( "//blp/refdata" ) then
        
            Some( session )

        else
        
            None

    with

    | _ as e -> raise( e )




let bloombergSecurityErrorsAndFieldExceptions ( msg : Message ) =

    let securityData = msg.GetElement("securityData")

    if securityData.HasElement( "securityError" ) then
        
        raise( Exception ( "bloomberg security Error: " + msg.ToString() ) )

    elif securityData.HasElement( "fieldExceptions" ) then

        let fieldExceptions = securityData.GetElement( "fieldExceptions" )
    
        if fieldExceptions.NumValues > 0 then

            let mutable txt = ""

            // can there really be more than 1 value given that we only pass one field per request ??
            for i in 0 .. fieldExceptions.NumValues - 1 do

                let element = fieldExceptions.GetValueAsElement( i )
                let fieldId = element.GetElementAsString( "fieldId" )
                let errorMessage = element.GetElement( "errorInfo" ).GetElementAsString( "message" )
        
                txt <- txt + "field Id: " + fieldId + errorMessage + " ;;; "

            raise( Exception( "bloomberg field exceptions: " + txt ) )




let bloombergProcessResponse ( eventObj : Event ) ( processFields : Message -> 'a[] ) =

    [|

        for msg in eventObj ->

            if msg.HasElement( "responseError" ) then

                let errorInfo =  msg.GetElement( "responseError" )

                raise( Exception( "bloomberg request failed: " +  errorInfo.GetElementAsString( "category" ) + " (" + errorInfo.GetElementAsString( "message" ) + ")" ) )

            else

                if eventObj.Type = Event.EventType.PARTIAL_RESPONSE || eventObj.Type = Event.EventType.RESPONSE then

                    bloombergSecurityErrorsAndFieldExceptions msg

                    processFields msg

                else

                    [||]

    |]

    |> Array.concat




let bloombergEventLoop ( session : Session ) processFields =

    try

        let mutable ret = Array.empty
        let mutable finished = false

        while not finished do 

            let eventObj = session.NextEvent();
            if eventObj.Type = Event.EventType.PARTIAL_RESPONSE then

                ret <- bloombergProcessResponse eventObj processFields |> Array.append ret 

            elif eventObj.Type = Event.EventType.RESPONSE then

                ret <- bloombergProcessResponse eventObj processFields |> Array.append ret 
                finished <- true

            else

                for msg in eventObj do

                    if eventObj.Type = Event.EventType.SESSION_STATUS then

                        if msg.MessageType.Equals "SessionTerminated" then

                            finished <- true;

        ret

    with

    | _ as e -> raise( e )