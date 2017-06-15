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


## Example Program
Here follows a simple example of how `TheDataMustFlow` might be used for machine learning (eventually we'll have macros to drastically simplify this:
```julia
# get a source, in this case a feather file
src = Feather.Source(filename)
src_sch = Data.schema(src)
nrows = size(src, 1)

# create a filter to determine which rows to use
idx = filterall(src, 1:nrows, Header1=(i -> i % 2 == 0),
                Header2=(i -> i % 3 == 0))

# create a harvester for extracting data
harvest = harvester(src, Float64, [:A, :C], [:B, :D])

# create a sink to put the data into
dtypes = [DataType[eltype(dt) for dt ∈ Data.types(src_sch)]; Float32; Float32]
header = [Symbol.(Data.header(src_sch)); :γ; :δ]
sink = DataTable(dtypes, header, nrows)

# migrate everything to an output table
migrate!(1:nrows, src=>sink)

# create Sower
sow! = sower(sink, [:γ, :δ])

for sidx ∈ batchiter(idx, batch_size)
    X, y = harvest(sidx)
    train!(model, X, y)
    ŷ = predict(model, X) # obviously this is silly, here only for demonstration purposes
    sow!(sidx, ŷ)
end
```


## API Docs
```@autodocs
Modules = [TheDataMustFlow]
Private = false
```


