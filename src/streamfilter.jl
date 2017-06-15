
# TODO for now everything is one row at a time, at some point we must do blocks

# this should be able to handle sources with nulls
"""
# Type: `StreamFilter <: AbstractMorphism{Pull}`

A `StreamFilter` is an implementation of an `AbstractMorphism{Pull}` which is designed
for determining which rows of a data table satisfy certain criteria.  It is intended that
one of these be used to determine which index arguments should be passed to other methods,
for example the results of `Morphism`, `Harvester` or `Sower`.


## Constructors

```julia
StreamFilter(s, cols::AbstractVector, filterfuncs::AbstractVector{Function};
             lift_nulls::Bool=true, logical_op::Function=(&))
StreamFilter(s; lift_nulls::Bool=true, logical_op::Function=(&);
             kwargs...)
```

## Arguments

- `s`: The source, which must be a tabular data format implementing the `DataStreams`
    interface.
- `cols`: The columns which are relevant to the filter.  This should be a vector of
    integers or symbols.
- `filterfuncs`: Functions which will be applied to each colum. These should act on a single
    column element and return `Bool`.
- `lift_nulls`: Whether the functions should be appied to `Nullable` or the elements they
    contain.  If `lift_nulls` is true, rows with nulls present will not be included.
- `logical_op`: The logical operator combining columns.  For example, if `(&)`, the results
    of the filtering will return only rows for which *all* functions return true.
- `kwargs...`: One can instead pass functions using the column they are to be associated
    with as a keyword.  For example `Column1=f1, Column2=f2`.


## Notes

The function which implements the `StreamFilter` can be obtained by doing `streamfilter`.
Alternatively, one may wish to run the filter on an entire dataset by doing `filterall`.


## Examples

```julia
sfilter = StreamFilter(src, Col1=(i -> i % 2 == 0), Col2=(i -> i % 3 == 0))
bfilt = streamfilter(sfilter, Bool)
bfilt(1:100)  # will return a Vector{Bool} which is only true where
              # Col1 is a multiple of 2 and Col2 is a multiple of 3 for rows 1 through 100

filt = streamfilter(sfilter)
filt(1:100)  # will return a Vector{Int} containing the numbers of rows where
             # Col1 is a multiple of 2 and Col2 is a multiple of 3 for rows 1 through 100
```

"""
struct StreamFilter <: AbstractMorphism{Pull}
    s::Any
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}

    function StreamFilter(s, sch::Data.Schema, cols::AbstractVector,
                          func::Function)
        cols = _handle_col_args(sch, cols)
        new(s, sch, Tuple[cols], Function[func])
    end
    function StreamFilter(s, cols::AbstractVector, func::Function)
        StreamFilter(s, Data.schema(s), cols, func)
    end

    # TODO more advanced interface for multiple columns to one function
    # would probably require a macro
end
export StreamFilter


#=========================================================================================
    <intermediate constructors>
=========================================================================================#
function _nolift_func(f::Function)
    g(v::AbstractArray) = f.(v)

    function g(v::NullableArray)
        o = BitArray(length(v))
        for i ∈ 1:length(v)
            o[i] = f(v[i])
        end
        o
    end

    g
end

function _combine_streamfilter_funcs(funcs::AbstractVector{<:Function},
                                     lift::Type{Val{false}}, logical_op::Function=(&))
    ℓ = length(funcs)
    funcs = _nolift_func.(funcs)
    function g(args::AbstractVector...)
        logical_op.((funcs[i](args[i]) for i ∈ 1:ℓ)...)
    end
end

function _combine_streamfilter_funcs(funcs::AbstractVector{<:Function}, lift::Type{Val{true}},
                                     logical_op::Function=(&))
    ℓ = length(funcs)
    function g(args::AbstractVector...)
        logical_op.((funcs[i].(args[i]) for i ∈ 1:ℓ)...)
    end
end


# this  constructor takes user functions and combines them
function StreamFilter(s, sch::Data.Schema, cols::AbstractVector{<:Integer},
                      filterfuncs::AbstractVector{<:Function};
                      lift_nulls::Bool=true, logical_op::Function=(&))
    StreamFilter(s, sch, cols,
                 _combine_streamfilter_funcs(filterfuncs, Val{lift_nulls}, logical_op))
end
function StreamFilter(s, cols::AbstractVector{<:Integer},
                      filterfuncs::AbstractVector{<:Function};
                      lift_nulls::Bool=true, logical_op::Function=(&))
    StreamFilter(s, Data.schema(s), cols, filterfuncs, lift_nulls=lift_nulls,
                 logical_op=logical_op)
end
function StreamFilter(s, sch::Data.Schema, cols::AbstractVector{Symbol},
                      filterfuncs::AbstractVector{<:Function};
                      lift_nulls::Bool=true, logical_op::Function=(&))
    StreamFilter(s, sch, colidx(sch, cols), filterfuncs, lift_nulls=lift_nulls,
                 logical_op=logical_op)
end
function StreamFilter(s, cols::AbstractVector{Symbol},
                      filterfuncs::AbstractVector{<:Function};
                      lift_nulls::Bool=true, logical_op::Function=(&))
    StreamFilter(s, Data.schema(s), cols, filterfuncs, lift_nulls=lift_nulls,
                 logical_op=logical_op)
end
#=========================================================================================
    </intermediate constructors>
=========================================================================================#


#=========================================================================================
    <advanced constructors>
=========================================================================================#
_streamfilter_handle_kwarg(f::Function) = f
_streamfilter_handle_kwarg(L::AbstractVector) = (x -> (x ∈ L))
# TODO any other cases to add?

function StreamFilter(src; lift_nulls::Bool=true, logical_op::Function=(&), kwargs...)
    cols = convert(Vector{Symbol}, getindex.(kwargs,1))
    funs = _streamfilter_handle_kwarg.(getindex.(kwargs,2))
    StreamFilter(src, cols, funs, lift_nulls=lift_nulls, logical_op=logical_op)
end
#=========================================================================================
    </advanced constructors>
=========================================================================================#



#=========================================================================================
    <basic functions>
=========================================================================================#
"""
    streamfilter(sf::StreamFilter[, ::Type{Bool}])

Returns a function that applies the functions provided by streamfilter to rows in a range.
The function returned can be called like `filt(idx)` where `idx` is an `AbstractVector`
of indices.  If `Bool` is passed, `filt` will return a `Vector{Bool}` with elements
that are true only for rows which satisfy the requirements.  Otherwise, a vector of the
row numbers will be returned.

Optionally, one can pass the arguments for the `StreamFilter` constructor directly to
this function.
"""
function streamfilter(sf::StreamFilter, ::Type{Bool})
    m = morphism(sf)  # returns single element tuple
    idx::AbstractVector{<:Integer} -> m(idx)[1]
end
function streamfilter(f::StreamFilter)
    func = streamfilter(f, Bool)
    idx::AbstractVector{<:Integer} -> find(func(idx)) + idx[1] - 1
end
function streamfilter(src, cols, funcs; kwargs...)
    streamfilter(StreamFilter(src, cols, funcs; kwargs...))
end
function streamfilter(src, cols, funcs, ::Type{Bool}; kwargs...)
    streamfilter(StreamFilter(src, cols, funcs; kwargs...), Bool)
end
streamfilter(src; kwargs...) = streamfilter(StreamFilter(src; kwargs...))
function streamfilter(src, ::Type{Bool}; kwargs...)
    streamfilter(StreamFilter(src; kwargs...), Bool)
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


# this accepts the stream filter function or the filter itself. Use with caution.
"""
    filterall(f::StreamFilter, idx::AbstractVector{<:Integer}; batch_size::Integer=DEFAULT)

Determine the indices selected by the `StreamFilter`.  These indices will be members of
`idx` for which the `StreamFilter` source satisfies the functions in was created with.
"""
function filterall{T<:Integer}(f::Union{Function,StreamFilter}, idx::AbstractVector{T};
                               batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    iter = batchiter(f, idx, batch_size=batch_size)
    o = Vector{T}()  # no way of predicting the size of this
    for idxo ∈ iter
        append!(o, idxo)
    end
    o
end
function filterall{T<:Integer}(src, cols::AbstractVector{Symbol},
                               funcs::AbstractVector{Function},
                               idx::AbstractVector{T};
                               lift_nulls::Bool=true, logical_op::Function=(&),
                               batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    filterall(StreamFilter(src, cols, funs, lift_nulls, logical_op),
              idx, batch_size=batch_size)
end
function filterall{T<:Integer}(src, idx::AbstractVector{T};
                               batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE,
                               kwargs...)
    filterall(StreamFilter(src; kwargs...), idx, batch_size=batch_size)
end
function filterall(f::StreamFilter; batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    filterall(f, 1:size(f,1), batch_size=batch_size)
end
function filterall(src; batch_size=DEFAULT_FILTER_BATCH_SIZE, kwargs...)
    sf = StreamFilter(src; kwargs...)
    filterall(sf, batch_size=batch_size)
end
export filterall


# conversions
Base.convert(::Type{Morphism{Pull}}, f::StreamFilter) = Morphism{Pull}(f.s, f.schema,
                                                                       f.cols, f.funcs)
Base.convert(::Type{Morphism}, f::StreamFilter) = Morphism{Pull}(f.s, f.schema,
                                                                 f.cols, f.funcs)
#=========================================================================================
    </basic functions>
=========================================================================================#


# TODO need interface functions, including those which act internally as StreamFilter constr

