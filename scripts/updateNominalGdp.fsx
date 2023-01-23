#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\marketData.dll"


#load@"./settingsDataFieldDictionary.fsx"



open System


open SA2.MarketData.BbNominalGdp
open SA2.Common.Dates
open SA2.Common.DbSchema



let ticker2Type =
    
    [
        ( "BZGDGDPQ Index" , NominalGdpTickerType.level ) ;
        ( "PRSRGDP Index" , NominalGdpTickerType.level ) ;
        ( "EUGDEU27 Index" , NominalGdpTickerType.level ) ;
        ( "BUGQTOTL Index" , NominalGdpTickerType.level ) ;
        ( "HUGQT Index" , NominalGdpTickerType.level ) ;
        ( "KUCUTOTA Index" , NominalGdpTickerType.level ) ;
        ( "OMGDP Index" , NominalGdpTickerType.level ) ;
        ( "SADXGDEN Index" , NominalGdpTickerType.level ) ;
        ( "UAGDGDPB Index" , NominalGdpTickerType.level ) ;
        ( "HKGCGDP Index" , NominalGdpTickerType.level ) ;
        ( "INXQGDPM Index" , NominalGdpTickerType.level ) ;
        ( "PAGCEXPN Index" , NominalGdpTickerType.level ) ;
        ( "PHGDPC$ Index" , NominalGdpTickerType.level ) ;
        ( "THG PC$Q Index" , NominalGdpTickerType.level ) ;
        ( "VEGCTOTL Index" , NominalGdpTickerType.level ) ;
        ( "CGEBQOQ Index" , NominalGdpTickerType.qoq ) ;
        ( "GDP CUAQ Index" , NominalGdpTickerType.qoq ) ;
        ( "CLGDQCUR Index" , NominalGdpTickerType.qoq ) ;
        ( "COCUPIBQ Index" , NominalGdpTickerType.qoq ) ;
        ( "MXNPSUNQ Index" , NominalGdpTickerType.qoq ) ;
        ( "ASGDGDPQ Index" , NominalGdpTickerType.qoq ) ;
        ( "BEGDPQCS Index" , NominalGdpTickerType.qoq ) ;
        ( "DEGDPCSQ Index" , NominalGdpTickerType.qoq ) ;
        ( "FIGDCRQ Index" , NominalGdpTickerType.qoq ) ;
        ( "FRNGGDPQ Index" , NominalGdpTickerType.qoq ) ;
        ( "GDPBGDQQ Index" , NominalGdpTickerType.qoq ) ;
        ( "GDGRIQQ Index" , NominalGdpTickerType.qoq ) ;
        ( "ICGPQOQ Index" , NominalGdpTickerType.qoq ) ;
        ( "IEGRCURQ Index" , NominalGdpTickerType.qoq ) ;
        ( "ITPINLQS Index" , NominalGdpTickerType.qoq ) ;
        ( "NEGDPSCQ Index" , NominalGdpTickerType.qoq ) ;
        ( "NOGDPCH Index" , NominalGdpTickerType.qoq ) ;
        ( "PTGDPQQC Index" , NominalGdpTickerType.qoq ) ;
        ( "SPNAGDPG Index" , NominalGdpTickerType.qoq ) ;
        ( "SWGCGDPQ Index" , NominalGdpTickerType.qoq ) ;
        ( "SZGNGDPQ Index" , NominalGdpTickerType.qoq ) ;
        ( "UKGRYBAQ Index" , NominalGdpTickerType.qoq ) ;
        ( "CZGDPCSQ Index" , NominalGdpTickerType.qoq ) ;
        ( "PODPQOQ Index" , NominalGdpTickerType.qoq ) ;
        ( "RODPQOQ Index" , NominalGdpTickerType.qoq ) ;
        ( "RUDPGLQQ Index" , NominalGdpTickerType.qoq ) ;
        ( "TUGPCUQQ Index" , NominalGdpTickerType.qoq ) ;
        ( "BJQGDPQ Index" , NominalGdpTickerType.qoq ) ;
        ( "ISGOPNQQ Index" , NominalGdpTickerType.qoq ) ;
        ( "QADPTOTQ Index" , NominalGdpTickerType.qoq ) ;
        ( "SRQGGDPQ Index" , NominalGdpTickerType.qoq ) ;
        ( "AUGDPCQ Index" , NominalGdpTickerType.qoq ) ;
        ( "IDGRQY Index" , NominalGdpTickerType.qoq ) ;
        ( "JGDOQOQ Index" , NominalGdpTickerType.qoq ) ;
        ( "MAGRTTQQ Index" , NominalGdpTickerType.qoq ) ;
        ( "NZNTNOMQ Index" , NominalGdpTickerType.qoq ) ;
        ( "SGDPCURQ Index" , NominalGdpTickerType.qoq ) ;
        ( "KOEGSTOQ Index" , NominalGdpTickerType.qoq ) ;
        ( "TWRGSANQ Index" , NominalGdpTickerType.qoq ) ;
        ( "CNNGPC$ Index" , NominalGdpTickerType.ytd ) ;

    ]

    |> Map.ofList



// execute

updateNominalGdp SettingsDataFieldDictionary.dataFieldDictionary ticker2Type DateFrequency.quarterly DatabaseFields.price 19700101 ( DateTime.Today |> obj2Date )
