using DataTables
using DataStreams
using TheDataMustFlow

const nrows = 10^6
const batch_size = 1024

makecol(n::Real) = (1.0:Float64(nrows)) + n

src = DataTable(Header1=1:nrows, Header2=1:nrows,
                A=makecol(0), B=makecol(10), C=makecol(100), D=makecol(1000))

filter = StreamFilter(src, [:Header1, :Header2],
                      Function[i -> (i % 2 == 0), i -> (i % 3 == 0)])
idx = index(filter, 1:nrows)


h = Harvester(src, [:A, :B], [:C, :D])

@time allpairs = [(X, y) for (X, y) ∈ harvest(h, idx, Float64, batch_size=batch_size)]


