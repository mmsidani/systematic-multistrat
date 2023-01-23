#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\equities.dll"
#r@"..\sa2-dlls\rates.dll"
#r@"..\sa2-dlls\options.dll"
#r@"..\3rdparty\MathNet.Numerics.dll"



#load ".\settingsProduction.fsx"


open MathNet.Numerics
open SA2.Common.Math
open SA2.Common.Utils
open SA2.Common.KMeans


let junk () =

    let bondFactorDrivers =

        [

        "USGG10YR INDEX"

        ]


    let equityFactorDrivers =

        [

        "RTY INDEX" ;
        "TPX INDEX" ;
        "SXXP INDEX" ;

        ]


    let roroDrivers =

        [

        bondFactorDrivers ;
        equityFactorDrivers

        ]

        |> List.concat

    let ranGen = new Random.MersenneTwister ()
////    let a = Array2D.init 10 10 ( fun _ _  -> ranGen.NextDouble () )
////
////    let factor = calculatePca a
////
////    printfn "\n\nsolution \n\n%A" (  minimizeExposureToSystemicFactor2 a.[ * , 0..6 ] a.[ *, 7 .. 9 ] factor )
//
//    let a = Array.init 10 ( fun _ -> ranGen.NextDouble () |> log )
//    printfn "a : \n\n%s" ( paste a )
//    printfn "result:\n\n%s" (hpFilter 1600.0 a |> paste)
//
//    let index0 = Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index1 = Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index2 = Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index3= Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index4= Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index5= Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index6= Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index7= Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index8 = Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index9= Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index10= Array.init 10 ( fun _ -> ranGen.NextDouble () )
//    let index11= Array.init 10 ( fun _ -> ranGen.NextDouble () )
//
//    [ ( "index0" , index0 );
//     ( "index1" , index1 );
//     ( "index2" , index2 );
//     ( "index3" , index3 );
//     ( "index4" , index4 );
//     ( "index5" , index5 );
//     ( "index6" , index6 );
//     ( "index7" , index7 );
//     ( "index8" , index8 );
//     ( "index9" , index9 );
//     ( "index10" , index10 );
//     ( "index11" , index11 );
//    ]
//    |> Map.ofList
//    |>     computeClusters 2 3 euclideanDistance


    let rans = Array.init 10 ( fun i -> ranGen.Next( -1 , 5 ) )
    printfn "rans %s" (paste rans)

junk () 