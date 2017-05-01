

"""
# Type: `Sower`

***TODO*** Documentation!!!
"""
struct Sower <: AbstractMorphism{PushForward}
    s::Any
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}


    function Sower(s, sch::Data.Schema,
                   cols::AbstractVector{<:Tuple},
                   funcs::AbstractVector{<:Function})
        cols = Tuple[_handle_col_args(sch, t) for t ∈ cols]
        new(s, sch, cols, funcs)
    end
    function Sower(s, cols::AbstractVector{<:Tuple},
                   funcs::AbstractVector{<:Function})
        new(s, Data.schema(s), cols, funcs)
    end
end
export Sower


#=========================================================================================
    <intermediate constructors>
=========================================================================================#
function Sower(s, sch::Data.Schema, cols::AbstractVector{Tuple})
    Sower(s, sch, cols, Function[identity for i ∈ 1:length(cols)])
end
function Sower(s, cols::AbstractVector{Tuple})
    Sower(s, Data.schema(s), cols)
end

function Sower(s, sch::Data.Schema, cols::AbstractVector{<:Integer})
    Sower(s, sch, Tuple[tuple(cols...)])
end
function Sower(s, cols::AbstractVector{<:Integer})
    Sower(s, Data.schema(s), cols)
end
function Sower(s, sch::Data.Schema, cols::AbstractVector{Symbol})
    Sower(s, sch, colidx(sch, cols))
end
function Sower(s, cols::AbstractVector{Symbol})
    Sower(s, Data.schema(s), cols)
end

Sower(s, cols::Symbol...) = Sower(s, collect(cols))
#=========================================================================================
    </intermediate constructrs>
=========================================================================================#


#=========================================================================================
    <sowing>
=========================================================================================#
"""
    sower(s::Sower)

**TODO** Documentation!
"""
sower(s::Sower) = morphism(s)
sower(s, cols::AbstractVector{<:Integer}) = sower(Sower(s, cols))
sower(s, cols::AbstractVector{Symbol}) = sower(Sower(s, cols))
export sower
#=========================================================================================
    </sowing>
=========================================================================================#



