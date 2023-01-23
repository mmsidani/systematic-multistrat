#r@"../sa2-dlls/common.dll"


    (*

        NOTE:

        run this in cmd window or even better double-click in windows explorer.

        ( the first way requires changing directories to where this file lives and this can be forgotten. the second way cannot be done without opening that folder first )

        why? we have 3 identically designed tables and maintaining 3 versions of the same code is not good

    *)



open System
open SA2.Common.Io


// input 

let private fileToClone = "./Table_equities_data.fs"

let private string2Replacement2File =

    [ 
        ( "equities" , "rates" , "./Table_rates_data.fs" ) ;
        ( "equities" , "macro" , "./Table_macro_data.fs" ) 
        
    ]

// end input



let cloneLogic () =

    let logicString = readFile fileToClone |> Seq.toList

    for i in 0 .. string2Replacement2File.Length - 1 do

        let stringToReplace , replacementString , fileName = string2Replacement2File.[ i ]

        let sw = new IO.StreamWriter ( fileName )


        logicString

            |> List.map ( fun l -> l.Replace ( stringToReplace , replacementString ) )

            |> List.iter ( fun l -> sw.WriteLine l )

        sw.Close () 




// execute

cloneLogic ()