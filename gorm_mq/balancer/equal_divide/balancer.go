package equal_divide

type Balancer struct {
}

func (b *Balancer) Balance(partition int, consumers int) [][]int {
	ans := make([][]int, consumers)
	for i := 0; i < partition; i++ {
		index := i % consumers
		ans[index] = append(ans[index], i)
	}
	return ans

}

func NewBalancer() *Balancer {
	return &Balancer{}
}
