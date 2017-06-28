#===================================================================================================
    TODO:

    I've been stressing out because I'd really like to come up with a much more general way of
    doing transformations that depend on the entire dataset.  Right now this is a very ad hoc,
    hackish thing and it cannot be easily extended to other purposes.

    I STILL HAVEN'T FIGURED OUT THE BEST WAY FOR THIS TO RETURN
===================================================================================================#


#=========================================================================================
    <survey>
=========================================================================================#
mutable struct Survey
    idx::AbstractVector
    catpools::Dict{Int,CategoricalPool}

    Survey() = new(Vector{Int}(), Dict{Int,CategoricalPool}())
end
export Survey
#=========================================================================================
    </survey>
=========================================================================================#


#=========================================================================================
    <surveyor>
=========================================================================================#
"""
# Type: `Surveyor <: AbstractMorphism{Pull}`

A `Surveyor` is an implementation of an `AbstractMorphism{Pull}` which is designed for gathering
metadata which is necessary prior to transformation into a machine ingestible format.  It is
intended that one of these be used to determine which index arguments should be passed to other
methods, as well as how to construct transformations that depend on the entire dataset (such as labeling
categorical variables).

## Constructors
```julia
Surveyor(s, cols::AbstractVector, funcs::AbstractVector{Function};
         lift_nulls::Bool=true, logical_op::Function=(&), pool_cols::AbstractVector=[])
Surveyor(s; lift_nulls::Bool=true, logical_op::Function=(&), pool_cols::AbstractVector=[],
         kwargs..._)
```

## Arguments
- `s`: The source, which must be in a tabular data format implementing the `DataStreams` interface.
- `cols`: The columns which are relevant to the filter.  This should be a vector of integers, strings
    or symbols.
- `funcs`: Functions which will be applied to each column. These should act on a single column element
    and return `Bool`.
- `lift_nulls`: Whether the functions should be applied to `Nullable` or the elements they contain.
    If` lift_nulls` is true, rows with nulls present will not be included.
- `logical_op`: The logical operator combining the output of the functions that act on the columns.
    For example, if `(&)`, the results of the filtering will return only rows for which *all* functions
    return true.
- `kwargs...`: One can isntead pass functions using the column they are to be associated with as a
    keyword.  For example `Column1=f1, Column2=f2`.
- `pool_cols`: Columns for which `CategoricalPool`s will be created. These are for mapping the categories
    to and from machine-ingestable integers.


## Notes

The function which implements the `Surveyor` can be obtained by doing `surveyor`.
Alternatively, one may wish to run the surveyor on an entire dataset by doing `surveyall`.

## Examples

```julia
svr = Surveyor(src, Col1=(i -> i % 2 == 0), Col2=(i -> i % 3 == 0))
sv = surveyor(svr)
sv(1:100)
```
"""
struct Surveyor <: AbstractMorphism{Pull}
    s::Any
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}

    surveys::Vector{Survey}
    poolcols::Vector{Int}

    function Surveyor(s, sch::Data.Schema, cols::AbstractVector, func::Function,
                      poolcols::Vector{Int}=Int[])
        cols = _handle_col_args(sch, cols)
        new(s, sch, Tuple[cols], Function[func], Survey[], poolcols)
    end
    function Surveyor(s, cols::AbstractVector, func::Function, poolcols::Vector{Int}=Int[])
        Surveyor(s, Data.schema(s), cols, func, poolcols)
    end
end
export Surveyor

getsurvey(sv::Surveyor, n::Integer) = sv.surveys[n]

addsurvey!(sv::Surveyor, surv::Survey) = push!(sv.surveys, surv)

Base.size(sv::Surveyor) = size(sv.schema)
Base.size(sv::Surveyor, i::Integer) = size(sv.schema, i)

function makesurvey!(sv::Surveyor)
    surv = Survey()
    for poolcol ∈ sv.poolcols
        dtype = eltype(eltype(sv.schema, poolcol)) # outer eltype in case this is a nullable
        surv.catpools[poolcol] = CategoricalPool(Vector{dtype}())
    end
    addsurvey!(sv, surv)[end]
end

function getpool(sv::Surveyor, col::Int, survey_number::Integer=-1)
    if survey_number < 0
        sv.surveys[end].catpools[col]
    else
        sv.surveys[survey_number].catpools[col]
    end
end
function getpool(sv::Surveyor, col::String, survey_number::Integer=-1)
    getpool(sv, sv.schema[col], survey_number)
end
getpool(sv::Surveyor, col::Symbol, survey_number::Integer=-1) = getpool(sv, string(col), survey_number)
export getpool

function Base.getindex(sv::Surveyor, survey_number::Integer=-1)
    if survey_number < 0
        sv.surveys[end].idx
    else
        sv.surveys[survey_number].idx
    end
end
#=========================================================================================
    </surveyor>
=========================================================================================#




#=========================================================================================
    <intermediate constructors>
=========================================================================================#
function _surveyor_pull_cols(cols::AbstractVector, pool_cols::AbstractVector)
    extra_cols = [c for c ∈ pool_cols if c ∉ cols]
    allcols = Int[cols; extra_cols]
    poolargs = findin(allcols, pool_cols)
    allcols, poolargs
end

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

function _combine_surveyor_funcs(funcs::AbstractVector{<:Function},
                                 poolcolargs::AbstractVector{<:Integer},
                                 lift::Bool, logical_op::Function=(&))
    ℓ = length(funcs)
    if !lift
        funcs = _nolift_func.(funcs)
    end
    function (args::AbstractVector...)
        o = logical_op.((funcs[i].(args[i]) for i ∈ 1:ℓ)...)
        o, Any[args[i][o] for i ∈ poolcolargs]  # fetch categories only for cols we want
    end
end

# TODO for now we are using this simple form. will eventually need a macro to do better stuff
function Surveyor(s, sch::Data.Schema, cols::AbstractVector, filterfuncs::AbstractVector{<:Function};
                  lift_nulls::Bool=true, logical_op::Function=(&),
                  pool_cols::AbstractVector=[])
    cols = _handle_col_args(Vector, sch, cols)
    pool_cols = _handle_col_args(Vector, sch, pool_cols)
    pull_cols, poolcolargs = _surveyor_pull_cols(cols, pool_cols)
    Surveyor(s, sch, pull_cols,
             _combine_surveyor_funcs(filterfuncs, poolcolargs, lift_nulls, logical_op), pool_cols)
end
function Surveyor(s, cols::AbstractVector, filterfuncs::AbstractVector{<:Function};
                  lift_nulls::Bool=true, logical_op::Function=(&),
                  pool_cols::AbstractVector=[])
    Surveyor(s, Data.schema(s), cols, filterfuncs, lift_nulls=lift_nulls, logical_op=logical_op,
             pool_cols=pool_cols)
end
#=========================================================================================
    </intermediate constructors>
=========================================================================================#


#=========================================================================================
    <advanced constructors>
=========================================================================================#
_surveyor_handle_kwarg(f::Function) = f
_surveyor_handle_kwarg(L::AbstractVector) = (x -> (x ∈ L))

function Surveyor(src; lift_nulls::Bool=true, logical_op::Function=(&), pool_cols::AbstractVector=[],
                  kwargs...)
    cols = convert(Vector{Symbol}, getindex.(kwargs,1))
    funs = _surveyor_handle_kwarg.(getindex.(kwargs,2))
    Surveyor(src, cols, funs; lift_nulls=lift_nulls, logical_op=logical_op, pool_cols=pool_cols)
end
#=========================================================================================
    </advanced constructors>
=========================================================================================#



#=========================================================================================
    <basic functions>

    # NOTE: it is still intended that the survey be run on the entire dataset
    # it is not possible to determine length of index vector ahead of time
=========================================================================================#
function surveyor(sv::Surveyor, ::Type{Bool})
    m = morphism(sv)

    function (idx::AbstractVector{<:Integer})
        (b, colsvec), = m(idx)
        b, colsvec
    end
end

# TODO add size estimation for survey idx return
function surveyor(sv::Surveyor; survey_number::Integer=-1)
    if survey_number < 1
        surv = makesurvey!(sv)
    else
        surv = getsurvey(sv, survey_number)
    end

    m = morphism(sv)

    function (idx::AbstractVector{<:Integer})
        (b, colsvec), = m(idx)
        newidx = find(b) + idx[1] - 1
        append!(surv.idx, newidx)

        for (i,poolcol) ∈ enumerate(sv.poolcols)
            append!(surv.catpools[poolcol], dropnull(colsvec[i]))
        end

        sv  # TODO reconsider how to do this
    end
end

function surveyor(src, cols, funcs; kwargs...)
    surveyor(Surveyor(src, cols, funcs; kwargs...))
end
function surveyor(src, cols, funcs, ::Type{Bool}; kwargs...)
    surveyor(Surveyor(src, cols, funcs; kwargs...), Bool)
end
surveyor(src; kwargs...) = surveyor(Surveyor(src; kwargs...))
function surveyor(src, ::Type{Bool}; kwargs...)
    surveyor(Surveyor(src; kwargs...), Bool)
end
export surveyor

# TODO currently this is not compatible with what svr returns
function batchiter(sv::Surveyor, idx::AbstractVector{<:Integer};
                   batch_size::Integer=DEFAULT_SURVEY_BATCH_SIZE)
    svr = surveyor(sv)
    batchiter(svr, idx, batch_size)
end


function surveyall{T<:Integer}(f::Union{Function,Surveyor}, idx::AbstractVector{T};
                               batch_size::Integer=DEFAULT_SURVEY_BATCH_SIZE)
    iter = batchiter(f, idx; batch_size=batch_size)
    o = Vector{T}()  # no way of predicting the size of this
    for idxo ∈ iter
        append!(o, idxo)
    end
    o
end
function surveyall{T<:Integer}(src, cols::AbstractVector, funcs::AbstractVector{<:Function},
                               idx::AbstractVector{T};
                               lift_nulls::Bool=true, logical_op::Function=(&),
                               pool_cols::AbstractVector=[],
                               batch_size::Integer=DEFAULT_SURVEY_BATCH_SIZE)
    surveyall(Surveyor(src, cols, funcs; lift_nulls=lift_nulls, logical_op=logical_op),
              idx, batch_size=batch_size)
end
function surveyall{T<:Integer}(src, idx::AbstractVector{T};
                               batch_size::Integer=DEFAULT_SURVEY_BATCH_SIZE, kwargs...)
    surveyall(Surveyor(src; kwargs...), idx, batch_size=batch_size)
end
function surveyall(sv::Surveyor; batch_size::Integer=DEFAULT_SURVEY_BATCH_SIZE)
    surveyall(sv, 1:size(sv,1), batch_size=batch_size)
end
export surveyall


# conversions
Base.convert(::Type{Morphism{Pull}}, sv::Surveyor) = Morphism{Pull}(sv.s, sv.schema, sv.cols, sv.funcs)
Base.convert(::Type{Morphism}, sv::Surveyor) = Morphism{Pull}(sv.s, sv.schema, sv.cols, sv.funcs)
#=========================================================================================
    </basic functions>
=========================================================================================#




