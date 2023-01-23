module SA2.Equity.IndexTypes


type IndexWeightConstraint =

    {

    cappedIndex : Map< string , float >

    priceWeightedIndex : Set< string >

    }




type IndexUniverse =

    {

    indexUniverse : string list

    }




type FilteredIndexes =

    {

    criterion : string

    filteredIndexes : string list

    }




type ClusteredIndexes =

    {

    criterion : string

    clusteredIndexes : ( string list ) list

    }

