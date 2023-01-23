module SA2.Common.Io

open System.IO
open Utils




let readFile ( fileName : string ) = 

    seq {

        use sr = new StreamReader( fileName )

        while not sr.EndOfStream do

            yield sr.ReadLine ()

        sr.Close()

        }




let readFileRemoveHeader ( fileName : string ) =

    readFile fileName 

        |> Seq.toList
        
        |> List.tail



    
let parseOptionData ( dataLines : seq< string >  ) = 

    let triplets =
    
        dataLines 
        
            |> Seq.map ( fun ps -> 

                            let subs = ps.Split( [| ',' |] )
                            ( int(subs.[0].Replace( "-", "" )) , ( subs.[1], float( subs.[2] ) ) )

                        )

            |> Array.ofSeq

    let dates = triplets |> Array.unzip |> fst |> Set.ofArray |> Set.toArray |> Array.sort

    dates |> Array.fold ( fun s d -> 
                                let dataForDate = Array.filter (fun t -> fst t = d ) triplets |> Array.map ( fun t -> snd t ) |> Map.ofArray
                                Map.add d dataForDate s 
                        ) Map.empty




let writeArray x = Array.fold ( fun s y -> s + y.ToString() + "," ) "" x




let writeTupleArray x = Array.fold ( fun s y -> s + ( fst y ).ToString() + "," + ( snd y ).ToString() + "," ) "" x



let logBBOutput ( sw : StreamWriter ) data =

    data
        |> Array.map ( fun ( d , n , v ) -> d.ToString() + "," + n.ToString() + "," + v.ToString() )
        |> Array.iter ( fun l -> sw.WriteLine ( l ) )

    data
    



let logBBIndexMembersOutput ( sw : StreamWriter ) data =

    data
        |> Array.map ( fun ( d , n , v , w ) -> d.ToString() + "," + n.ToString() + "," + v.ToString() + "," + w.ToString() )
        |> Array.iter ( fun l -> sw.WriteLine ( l ) )

    data




let outputPortfolioFile instruments ( portfolioFileName : string ) ( portfolios : ( int * Map< string , float > ) [] ) =

    let longShortNames ( name : string ) =

        ( name.Split '|' ) |> ( fun a -> ( a.[ 0 ] , a.[ 1 ] ))


    let brokenForwardPortfolio = // break forward rates into their legs
        
        portfolios

            |> Array.map ( fun ( d , m ) -> 
                            ( d , m |> Map.fold ( fun s k w  -> 
                                                    if k.Contains "|" then
                                                        let longName , shortName = longShortNames k
                                                        Map.add longName w s |> Map.add shortName ( -w ) 
                                                    else
                                                        Map.add k w s )  Map.empty ) )

    let brokenInstruments = // break fwds into legs

        instruments
            |> List.map ( fun ( i : string ) -> 
                            if i.Contains "|" then
                                let long , short = longShortNames i
                                [ long ; short ]
                            else
                                [ i ] )

            |> List.concat
        
    let sw = new StreamWriter ( portfolioFileName )

    brokenInstruments
        |> List.reduce ( fun i0 i1 -> i0 + "," + i1 )
        |> ( fun header -> "Date," + header |> sw.WriteLine )

    brokenForwardPortfolio
        |> Array.map ( fun ( d , m ) -> 

                                d.ToString () + "," +
                                            (
                                                [|
                                                    for i in brokenInstruments ->
                                                        if Map.containsKey i m then
                                                            ( Map.find i m ).ToString()
                                                        else
                                                            "0.0"
                                                |]

                                                |> Array.reduce ( fun w0 w1 -> w0 + "," + w1 )
                                            )
                        )

        |> Array.iter ( fun l -> sw.WriteLine l )

    sw.Close ()