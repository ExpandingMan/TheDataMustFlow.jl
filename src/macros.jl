
#===================================================================================================
    <@morph>
===================================================================================================#
_morph_fix_col_arg(arg::Expr) = arg.args[1]
_morph_fix_col_arg(arg::QuoteNode) = arg.value
_morph_fix_col_arg(arg) = col

function _morph_parse_args(args::Vector)
    cols = Vector{Any}(length(args))
    newargs = Vector{Expr}(length(args))
    for (i,arg) ∈ enumerate(args)
        if @capture(arg, n_::Col{col_})
            cols[i] = _morph_fix_col_arg(col)
            newargs[i] = :($n::AbstractVector)
        else
            println(args)
            throw(ArgumentError("Invalid morphism argument $arg."))
        end
    end
    cols, Expr(:tuple, newargs...)
end

function _morph_parse_args(func::Expr)
    if @capture(func, function (args__,) body_ end | (args__,) -> body_)
        cols, args = _morph_parse_args(args)
        cols, args, body
    else
        [], Expr(:tuple), Expr(:tuple)
    end
end

function _morph(m, cols::AbstractVector, args::Expr, body::Expr)
    esc(quote
        TheDataMustFlow.addfunc!($m, $cols, $args -> $body)
    end)
end

function _morph(m, func::Expr)
    cols, args, body = _morph_parse_args(func)
    if length(cols) == 0
        return func
    end
    _morph(m, cols, args, body)
end

_morph(m, func) = func


"""
    @morph(m, block::Expr)

Appends functions to the Morphism object `m`.  Every anonymous function appearing in the block will
be added to `m`.  Functions should take arguments with the special type `Col{column_name}` where
`column_name` is an integer, string or symbol designating the column to be used as an argument.

For example
```julia
@morph M (a::Col{:A}, b::Col{:B}) -> a .+ b

@morph M begin
    function (a::Col{:A}, b::Col{:B})
        a .- b
    end

    (c::Col{:C}, d::Col{:D}) -> c .* d
end
```
Functions not matching this pattern will be unchanged.
"""
macro morph(m, block::Expr)
    MacroTools.postwalk(x -> _morph(m, x), block)
end
export @morph
#===================================================================================================
    </@morph>
===================================================================================================#


"""
    @morphism(direction::Symbol, src, block::Expr)

Create a morphism using the source or sink `src` and functions given in `block`.  See `@morph`
documentation for valid forms of `block`.

Example:
```julia
m = @morphism Pull src begin
    function (a::Col{:A}, b::Col{:B})
        a .- b
    end
end
```
"""
macro morphism(direction::Symbol, src, block::Expr)
    mname = gensym()  # we still escape so that stuff inside block gets preserved
    esc(quote
        $mname = Morphism{$direction}($src)
        @morph $mname $block
        $mname
    end)
end
export @morphism


