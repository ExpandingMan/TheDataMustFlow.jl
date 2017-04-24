

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


#=========================================================================================
    <migrating>
=========================================================================================#
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
    migrator!(s::Sower, src[, cols::AbstractVector{Symbol}], idx::AbstractVector{<:Integer};
              name_map::Dict=Dict(), batch_size::Integer=DEFAULT,
              index_map::Function=identity)

Returns a function `m!(idx)` that transfers columns `cols` from the source `src`
(which must implement the `DataStreams` source interface) to the `Sower`s sink.
This will occur only for the index `idx` of the source.  If the columns in the
sink have different names from those in the source, entries should appear in
`name_map` in the form `source_name=>sink_name`.  The transfer will be done in
batches of size `batch_size`.  The index `i` to which data is written will be
`index_map(α)` where `α` is the source index.  If the `cols` argument is
omitted, all columns from the source will be used.
"""
function migrator(s::Sower, src, cols::AbstractVector{Symbol};
                  name_map::Dict=Dict(),
                  index_map::Function=identity)
    sch = Data.schema(src)
    mcols = colidx(sch, cols)
    colmap = Dict()
    for (mcol, mcolname) ∈ zip(mcols, cols)
        colmap[mcol] = colidx(sch, get(name_map, mcolname, mcolname))
    end
    fromtypes = Data.types(sch)
    fromtypes = Dict(i=>vectortype(fromtypes[i]) for (i,v) ∈ colmap)
    totypes = Data.types(s.schema)
    totypes = Dict(i=>vectortype(totypes[i]) for (k,i) ∈ colmap)
    idx::AbstractVector{<:Integer} -> _migrate_batch!(s, src, colmap, fromtypes, totypes,
                                                      idx, index_map)
end
function migrator(s::Sower, src; name_map::Dict=Dict(),
                  batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                  index_map::Function=identity)
    migrator(s, src, Symbol.(Data.header(Data.schema(src))), name_map=name_map,
             index_map=index_map)
end
export migrator

# this is the iterator for migrating
function batchiter(s::Sower, src, cols::AbstractVector{Symbol},
                   idx::AbstractVector{<:Integer};
                   name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                   index_map::Function=identity)
    m! = migrator(s, src, cols, name_map=name_map, index_map=index_map)
    batchiter(m!, idx, batch_size)
end
function batchiter(s::Sower, src, idx::AbstractVector{<:Integer};
                   name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                   index_map::Function=identity)
    batchiter(s, src, Symbol.(Data.header(data.schema(src))), name_map=name_map,
              index_map=index_map, batch_size=batch_size)
end


# this does the full migration
function migrate!(s::Sower, src, cols::AbstractVector{Symbol},
                  idx::AbstractVector{<:Integer};
                  name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                  index_map::Function=identity)
    iter = batchiter(s, src, cols, idx, name_map=name_map, batch_size=batch_size,
                     index_map=index_map)
    foreach(x -> nothing, iter)
end
function migrate!(s::Sower, src, idx::AbstractVector{<:Integer};
                  name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                  index_map::Function=identity)
    migrate!(s, src, Symbol.(Data.header(Data.schema(src))), idx, name_map=name_map,
             batch_size=batch_size, index_map=index_map)
end
export migrate!
#=========================================================================================
    </migrating>
=========================================================================================#



#=========================================================================================
    <sowing>
=========================================================================================#
# this is largely a function boundary for efficiency
function _sow_column!{T,To}(s::Sower, ycol::Vector{T}, ::Type{To},
                            idx::AbstractVector{<:Integer}, tocol::Integer)
    streamto!(s.snk, Data.Column, convert(To, ycol), idx, tocol, s.schema)
end
# it is expected that this idx and y are not for the complete dataset
# this is expected to be in an external loop for lots of different y batches
"""
    sower(s::Sower)

Returns a function `s!(idx, y)` which places the contents of `y` into the indices `idx` of
the `Sower`'s sink.
"""
function sower(s::Sower)
    tocols = colidx(s.schema, s.newycols)
    totypes = Data.types(s.schema)
    totypes = Dict(i=>vectortype(totypes[i]) for i ∈ tocols)
    function (idx::AbstractVector{<:Integer}, y::Matrix)
        for col ∈ 1:size(y,2)
            tocol = tocols[col]
            _sow_column!(s, y[:,col], totypes[tocol], idx, tocol)
        end
    end
end
export sower

# note that this doesn't get an iterator because it takes arguments

#=========================================================================================
    </sowing>
=========================================================================================#



