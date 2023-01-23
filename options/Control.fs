module SA2.Options.Control

open SA2.Common.Utils
open Options

let transactionCosts tcPerContract ( port : Map<string, OptionPortfolio > ) =

    ( 0.0 , port ) ||> Map.fold ( fun s _ v -> ( v.shares |> Array.sum ) * tcPerContract + s ) |> abs

let avgCov ( weights : float[] ) ( cov : float[,] ) =
    
    if weights.Length <> Array2D.length1 cov || Array2D.length1 cov <> Array2D.length2 cov then

        raise ( ["mismatch between weights and covariance matrix or matrix is not square"] |> concatFileInfo __LINE__ |> VarianceException )

    Array.mapi ( fun i w  -> Array.mapi ( fun j u -> cov.[ i , j ] * w * u ) weights |> Array.sum ) weights |> Array.sum

//let rebalanceOnShift portfolios0 portfolios1 =
//
//    