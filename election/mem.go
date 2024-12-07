package election

// MemElection is an in-memory implementation for testing
type MemElection struct{}

func newMemElection() (Election, error) {
	return &MemElection{}, nil
}

func (e *MemElection) Close() error {
	return nil
}

func (m *MemElection) Resign() error {
	return nil
}

func (m *MemElection) Campaign() error {
	return nil
}

func (m *MemElection) IsActive() bool {
	return true
}
