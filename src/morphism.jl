

struct PullBack <: MapDirection end
struct PushForward <: MapDirection end
export PullBack, PushForward



struct Morphism{T<:MapDirection} <: AbstractMorphism{T}
    s::Any  # this can be either a source or a sink
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}

    # may add a new field for conversions

    function Morphism{T}(s, sch::Data.Schema, cols::AbstractVector{<:Tuple},
                         funcs::AbstractVector{<:Function}) where T
        new(s, sch, cols, funcs)
    end
    function Morphism{T}(s, cols::AbstractVector{<:Tuple},
                         funcs::AbstractVector{<:Function}) where T
        new(s, Data.schema(s), cols, funcs)
    end

    function Morphism{T}(s, sch::Data.schema, cols::AbstractVector{Symbol},
                         funcs::AbstractVector{<:Function}) where T
        cols = [tuple((sch[string(c)] for c ∈ co)...) for co ∈ cols]
        Morphism{T}(s, sch, cols, funcs)
    end
    function Morphism{T}(s, cols::AbstractVector{Symbol},
                         funcs::AbstractVector{<:Function}) where T
        Morphism{T}(s, Data.schema(s), cols, func)
    end

    # TODO make constructor macros
end
export Morphism


# returns function
function morphism{R}(m::AbstractMorphism, ::Type{R}=Tuple)
    if length(m.funcs) == 1
        return _morphism_single(m, 1)
    end
    _morphism_multi(m, R)
end
export morphism



#=========================================================================================
    <PullBack>
=========================================================================================#
function _morphism_return_func(m::AbstractMorphism{PullBack}, colstypes::Vector,
                               colmaps::Vector, nargs::Vector, ::Type{Vector})
    ℓ = length(m.cols)
    function (idx::AbstractVector{<:Integer})
        o = Vector{Any}(ℓ)
        cols = Any[streamfrom(m.s, Data.Column, dtype, idx, c) for (c,dtype) ∈ colstypes]
        for i ∈ 1:ℓ
            o[i] = m.funcs[i]((cols[colmaps[i][j]] for j ∈ 1:nargs[i])...)
        end
        o
    end
end
function _morphism_return_func(m::AbstractMorphism{PullBack}, colstypes::Vector,
                               colmaps::Vector, nargs::Vector, ::Type{Tuple})
    ℓ = length(m.cols)
    function (idx::AbstractVector{<:Integer})
        cols = Any[streamfrom(m.s, Data.Column, dtype, idx, c) for (c,dtype) ∈ colstypes]
        tuple((m.funcs[i]((cols[colmaps[i][j]] for j ∈ 1:nargs[i])...)
               for i ∈ 1:ℓ)...)
    end
end


function _morphism_single(m::AbstractMorphism{PullBack}, i::Integer)
    cols = m.cols[i]
    f = m.funcs[i]
    dtypes = coltypes(m.schema, cols)
    colstypes = collect(zip(cols, dtypes))

    function (idx::AbstractVector{<:Integer})
        f((streamfrom(m.s, Data.Column, dtype, idx, c)
           for (c,dtype) ∈ colstypes)...)
    end
end

function _morphism_multi{R}(m::AbstractMorphism{PullBack}, ::Type{R})
    allcols = collect(∪((Set(a) for a ∈ m.cols)...))
    alltypes = coltypes(m.schema, allcols)
    colstypes = collect(zip(allcols, alltypes))
    colmaps = [Dict(i=>findfirst(allcols, a[i]) for i ∈ 1:length(a)) for a ∈ m.cols]
    nargs = [length(a) for a ∈ m.cols]

    _morphism_return_func(m, colstypes, colmaps, nargs, R)
end


#=========================================================================================
    </PullBack>
=========================================================================================#



#=========================================================================================
    <PushForward>
=========================================================================================#

#=========================================================================================
    </PushForward>
=========================================================================================#



