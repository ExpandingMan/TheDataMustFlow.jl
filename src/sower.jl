

"""
# Type: `Sower <: AbstractMorphism{Push}`

This is a type for injecting data into a tabular data sink that implements the `DataStreams`
sink interface.

## Constructors

```julia
Sower(s, sch::Data.Schema, cols::AbstractVector)
Sower(s, cols::AbstractVector)
```

## Arguments

- `s`: The tabular data sink to inject data into. Must implement the `DataStreams` sink
    interface.
- `sch`: A `Data.Schema` for `s`. If it is not provided, it will be generated.
- `cols`: The columns the provided matrices will be injected into. If multiple matrices
    are being provided, the elements of this should be tuples.


## Examples

```julia
X = rand(100, 2)

sow = sower(sink, [:γ, :δ])  # can bypass Sower constructor
sow!(1:100, X)  # the first column of X goes to :γ, second column goes to :δ
```
"""
struct Sower <: AbstractMorphism{Push}
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
        Sower(s, Data.schema(s), cols, funcs)
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

function Sower(s, sch::Data.Schema, cols::AbstractVector)
    Sower(s, sch, Tuple[tuple(cols...)])
end
function Sower(s, cols::AbstractVector)
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

Returns a function `sow(idx, X...)` which will accept matrices `X` and map them into the
rows specified by `idx` and columns specified by the `Sower`.
"""
sower(s::Sower) = morphism(s)
sower(s, cols::AbstractVector) = sower(Sower(s, cols))
export sower
#=========================================================================================
    </sowing>
=========================================================================================#



