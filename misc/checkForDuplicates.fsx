#r@"..\sa2-dlls\common.dll"


open SA2.Common.Utils
open SA2.Common.Table_instruments


let checkForInstrumentsDuplicates () =

    // remove exchange code and EQUITY key and map to full ticker

    let allNames = getAllTickers_instruments () |> List.map ( fun n -> ( n.Split( [| ' ' |] ).[ 0 ] , n ) ) |> buildMap2List

    // sort descriptions and take seq.pairwise to see if any was duplicated. 

    let allDescriptions = getAllDescriptions_instruments () |> List.sort |> Seq.pairwise

    allDescriptions

        |> Seq.iter ( 
        
                    fun ( d0 , d1 ) -> 
        
                        if d0 = d1 then

                            printfn "%s is duplicated: %s " d0 d1

                    )

    // Note: we removed the exchange code above and so some of what we flag here as "duplicate" might not be one 

    allNames

        |> Map.iter ( 
        
                    fun n l -> 
        
                        if l.Length <> 1 then

                            printfn "%s was duplicated %d times : \n%s" n l.Length ( paste l )

                    )




// execute

checkForInstrumentsDuplicates ()