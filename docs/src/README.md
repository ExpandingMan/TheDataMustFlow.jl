# TheDataMustFlow

[![Build Status](https://travis-ci.org/ExpandingMan/TheDataMustFlow.jl.svg?branch=master)](https://travis-ci.org/ExpandingMan/TheDataMustFlow.jl)

[![Coverage Status](https://coveralls.io/repos/ExpandingMan/TheDataMustFlow.jl/badge.svg?branch=master&service=github)](https://coveralls.io/github/ExpandingMan/TheDataMustFlow.jl?branch=master)

[![codecov.io](http://codecov.io/github/ExpandingMan/TheDataMustFlow.jl/coverage.svg?branch=master)](http://codecov.io/github/ExpandingMan/TheDataMustFlow.jl?branch=master)

> "Without the data the navigators will be blind, the Bene Gesserit will lose all power, civilization will end.  If I am not obeyed, the data will not flow."
> -Muad'Dib

`TheDataMustFlow` is a package for applying transformations to array-indexable tabular data and collecting said data into arrays.  By "array-indexable" we mean
that the row and column of a datum can be specified by an integer pair.  It is assumed that the use of `TheDataMustFlow` will be preceded by some data
manipulation which involves joins and or aggregations, but is otherwise fairly minimal.  `TheDataMustFlow` is designed to address the difficulty of transforming
data into machine-readable arrays but also has general functionality for mapping between tabular formats.


## API Docs
```@autodocs
Modules = [TheDataMustFlow]
Private = false
```

