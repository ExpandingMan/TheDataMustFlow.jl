
# TODO for now everything is one row at a time, at some point we must do blocks

# this should be able to handle sources with nulls
"""
# Type: `StreamFilter`

This data type wraps an object `src` that implements the `DataStreams` source interface.
It is used for determining which rows of the source satisfy certain conditions.

## Constructors

    StreamFilter(src, filtercols::Vector{Symbol}, filterfuncs::Vector{Function})

One should pass a source object for which the appropriate rows will be determined.
The functions in `filterfuncs` should be functions which apply to elements of the respective
column given in `filtercols` and return booleans.  Indices for rows for which all of these
booleans are true are returned.
"""
struct StreamFilter <: AbstractStreamFilter
    src::Any
    schema::Data.Schema

    filtercols::Vector{Symbol}

    filterfuncs::Vector{Function}


    function StreamFilter(src, sch::Data.Schema, filtercols::AbstractVector{Symbol},
                          filterfuncs::AbstractVector{Function})
        if length(filtercols) ≠ length(filterfuncs)
            throw(ArgumentError("Must supply same number of filter functions as columns."))
        end
        new(src, sch, convert(Vector{Symbol}, filtercols),
            convert(Vector{Function}, filterfuncs))
    end

    function StreamFilter(src, filtercols::AbstractVector{Symbol},
                          filterfuncs::AbstractVector{Function})
        StreamFilter(src, Data.schema(src), filtercols, filterfuncs)
    end
end
export StreamFilter


#=========================================================================================
    <basic functions>
=========================================================================================#
colidx(f::StreamFilter) = colidx(f.schema, f.filtercols)

"""
    rowtype(f::StreamFilter)

Gets a `Tuple` type of the row in the source of `f`.
"""
function rowtype(f::StreamFilter)
    header = Data.header(f.schema)
    dtypes = Data.types(f.schema)
    filtercols = String[string(c) for c ∈ f.filtercols]
    idx = findin(header, filtercols)
    Tuple{dtypes[idx]...}
end


# helper function for index
function _index_batch(f::StreamFilter, cols::AbstractVector{<:Integer},
                      ctypes::AbstractVector{DataType},
                      batch_idx::AbstractVector{<:Integer})
    allcols = Vector{AbstractVector{Bool}}(length(f.filtercols))
    for i ∈ 1:length(f.filtercols)
        allcols[i] = sift(f.src, f.filterfuncs[i], ctypes[i], cols[i],
                          batch_idx)
    end
    mask = .&(allcols...)
    find(mask) + batch_idx[1] - 1
end


"""
    streamfilter(f::StreamFilter)

Return a function `I(idx)` which searches through the indices `idx` of the `StreamFilter`'s
source for rows satisfying the specified conditions.
"""
function streamfilter(f::StreamFilter)
    cols = colidx(f)
    ctypes = coltypes(f.schema, cols)
    idx::AbstractVector{<:Integer} -> _index_batch(f, cols, ctypes, idx)
end
export streamfilter


"""
## `StreamFilter`

    batchiter(f::StreamFilter, idx::AbstractVector{<:Integer}; batch_size::Integer=DEFAULT)

Returns an iterator over batches returning the valid indices within each batch.
"""
function batchiter(f::StreamFilter, idx::AbstractVector{<:Integer};
                   batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    filt = streamfilter(f)
    batchiter(filt, idx, batch_size)
end


"""
    filterall(f::StreamFilter, idx::AbstractVector{<:Integer}; batch_size::Integer=DEFAULT)

Determine the indices determined by the stream filter.  These indices will be members of
`idx` for which the `StreamFilter` source satisfies the functions in was created with.
"""
function filterall{T<:Integer}(f::StreamFilter, idx::AbstractVector{T};
                               batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    iter = batchiter(f, idx, batch_size=batch_size)
    o = Vector{T}()  # no way of predicting the size of this
    for idxo ∈ iter
        append!(o, idxo)
    end
    o
end
export filterall
#=========================================================================================
    </basic functions>
=========================================================================================#


# TODO need interface functions, including those which act internally as StreamFilter constr

