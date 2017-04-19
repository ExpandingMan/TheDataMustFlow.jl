using DataTables
using DataStreams
using Feather
using TheDataMustFlow

const filename = "sample.feather"
const batch_size = 1024

src = Feather.Source(filename)
nrows = size(src, 1)


filter = StreamFilter(src, [:Header1, :Header2],
                      Function[i -> (i % 2 == 0), i -> (i % 3 == 0)])
@time idx = index(filter, 1:nrows)


h = Harvester(src, [:A, :B], [:C, :D])

@time allpairs = [(X, y) for (X, y) ∈ harvest(h, idx, Float64, batch_size=batch_size)]


