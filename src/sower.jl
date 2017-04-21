

"""
# Type: `Sower`

This data type wraps an object `snk` that implements the `DataStreams` sink interface.
It is used for transfering machine learning data back into datasets.

See, for example, `migrate!` and `sow!`.

## Constructors

    Sower(sink, schema, newycols)
    Sower(sink, newycols)

One should pass a sink object which data will be written to.  Typically that data will be
in the form of a raw matrix (presumably some sort of machine learning output).  The columns
names passed should be the names into which the columns of the matrices should be
transferred.
"""
struct Sower <: AbstractSower
    snk::Any
    schema::Data.Schema

    newycols::Vector{Symbol}

    function Sower(sink, schema::Data.Schema, newycols::AbstractVector{Symbol})
        sinkcols = Symbol.(Data.header(schema))
        # for now we require that the newycols are already in the sink
        if !all(yc ∈ sinkcols for yc ∈ newycols)
            throw(ArgumentError("New y columns must already exist in sink."))
        end
        new(sink, schema, newycols)
    end
    function Sower(sink, newycols::AbstractVector{Symbol})
        Sower(sink, Data.schema(sink), newycols)
    end
end
export Sower


function _migrate_column!{From,To}(s::Sower, src, fromcol::Integer, tocol::Integer,
                                   ::Type{From}, ::Type{To},
                                   batch_idx::AbstractVector{<:Integer},
                                   index_map::Function)
    v = streamfrom(src, Data.Column, From, batch_idx, fromcol)
    v = convert(To, v)
    streamto!(s.snk, Data.Column, v, index_map.(batch_idx), tocol, s.schema)
end
function _migrate_batch!(s::Sower, src, colmap::Dict, fromtypes::Dict, totypes::Dict,
                         batch_idx::AbstractVector{<:Integer}, index_map::Function)
    for (fromcol, tocol) ∈ colmap
        _migrate_column!(s, src, fromcol, tocol, fromtypes[fromcol], totypes[tocol],
                         batch_idx, index_map)
    end
end

"""
    migrate!(s::Sower, src[, cols::AbstractVector{Symbol}], idx::AbstractVector{<:Integer};
             namemap::Dict=Dict(), batch_size::Integer=DEFAULT, index_map::Function=identity)

Transfers columns `cols` from the source `src` (which must implement the `DataStreams` source
interface) to the `Sower`s sink.  This will occur only for the index `idx` of the source.
If the columns in the sink have different names from those in the source, entries should
appear in `namemap` in the form `source_name=>sink_name`.  The transfer will be done in
batches of size `batch_size`.  The index `i` to which data is written will be
`index_map(α)` where `α` is the source index.  If the `cols` argument is omitted, all columns
from the source will be used.
"""
function migrate!(s::Sower, src, cols::AbstractVector{Symbol},
                  idx::AbstractVector{<:Integer}; namemap::Dict=Dict(),
                  batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                  index_map::Function=identity)
    sch = Data.schema(src)
    mcols = colidx(sch, cols)
    colmap = Dict()
    for (mcol, mcolname) ∈ zip(mcols, cols)
        colmap[mcol] = colidx(sch, get(namemap, mcolname, mcolname))
    end
    fromtypes = Data.types(sch)
    fromtypes = Dict(i=>vectortype(fromtypes[i]) for (i,v) ∈ colmap)
    totypes = Data.types(s.schema)
    totypes = Dict(i=>vectortype(totypes[i]) for (k,i) ∈ colmap)
    for batch_idx ∈ batchiter(idx, batch_size)
        _migrate_batch!(s, src, colmap, fromtypes, totypes, batch_idx, index_map)
    end
end
function migrate!(s::Sower, src, idx::AbstractVector{<:Integer}; namemap::Dict=Dict(),
                  batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                  index_map::Function=identity)
    migrate!(s, src, Symbol.(Data.header(Data.schema(src))), idx, namemap=namemap,
             batch_size=batch_size, index_map=index_map)
end
export migrate!


# this is largely a function boundary for efficiency
function _sow_column!{T,To}(s::Sower, ycol::Vector{T}, ::Type{To},
                              idx::AbstractVector{<:Integer}, tocol::Integer)
    streamto!(s.snk, Data.Column, convert(To, ycol), idx, tocol, s.schema)
end
# it is expected that this idx and y are not for the complete dataset
# this is expected to be in an external loop for lots of different y batches
"""
    sow!(s::Sower, idx::AbstractVecto{<:Integer}, y::Matrix{T})

Place the contents of `y` into the `Sower`s sink, in the indices given by `idx`.  Recall
that the columns the `y` are placed into are designated when the `Sower` is constructed.
Note that this function can easily be used in a `harvest` loop.
"""
function sow!{T}(s::Sower, idx::AbstractVector{<:Integer}, y::Matrix{T})
    tocols = colidx(s.schema, s.newycols)
    totypes = Data.types(s.schema)
    totypes = Dict(i=>vectortype(totypes[i]) for i ∈ tocols)
    for col ∈ 1:size(y,2)
        tocol = tocols[col]
        _sow_column!(s, y[:,col], totypes[tocol], idx, tocol)
    end
end
export sow!



