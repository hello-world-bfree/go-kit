package concurrency

import (
	"context"
	"sync"
)

// Pipeline represents a series of processing stages
type Pipeline[T any] struct {
	stages []Stage[T]
	ctx    context.Context
	cancel context.CancelFunc
}

// Stage represents a single stage in the pipeline
type Stage[T any] struct {
	Name    string
	Workers int
	Process func(context.Context, T) (T, error)
}

// NewPipeline creates a new pipeline
func NewPipeline[T any](stages ...Stage[T]) *Pipeline[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pipeline[T]{
		stages: stages,
		ctx:    ctx,
		cancel: cancel,
	}
}

// NewPipelineWithContext creates a pipeline with custom context
func NewPipelineWithContext[T any](ctx context.Context, stages ...Stage[T]) *Pipeline[T] {
	ctx, cancel := context.WithCancel(ctx)
	return &Pipeline[T]{
		stages: stages,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Execute runs the pipeline on a stream of items
func (p *Pipeline[T]) Execute(input <-chan T) (<-chan T, <-chan error) {
	output := make(chan T)
	errors := make(chan error, 10)

	go func() {
		defer close(output)
		defer close(errors)

		current := input
		for i, stage := range p.stages {
			isLast := i == len(p.stages)-1
			current = p.executeStage(stage, current, errors, isLast, output)
		}

		// Drain the last stage
		for range current {
		}
	}()

	return output, errors
}

// executeStage executes a single pipeline stage
func (p *Pipeline[T]) executeStage(stage Stage[T], input <-chan T, errors chan<- error, isLast bool, finalOutput chan<- T) <-chan T {
	output := make(chan T, stage.Workers)

	var wg sync.WaitGroup

	// Start workers for this stage
	for i := 0; i < stage.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for item := range input {
				select {
				case <-p.ctx.Done():
					return
				default:
					result, err := stage.Process(p.ctx, item)
					if err != nil {
						select {
						case errors <- err:
						default:
							// Error channel full
						}
						continue
					}

					if isLast {
						select {
						case finalOutput <- result:
						case <-p.ctx.Done():
							return
						}
					} else {
						select {
						case output <- result:
						case <-p.ctx.Done():
							return
						}
					}
				}
			}
		}()
	}

	// Close output channel when all workers are done
	go func() {
		wg.Wait()
		if !isLast {
			close(output)
		}
	}()

	return output
}

// Cancel stops the pipeline
func (p *Pipeline[T]) Cancel() {
	p.cancel()
}

// FanOut splits input across multiple processing functions
func FanOut[T any](ctx context.Context, input <-chan T, workers int, processor func(context.Context, T) error) <-chan error {
	errors := make(chan error, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range input {
				if err := processor(ctx, item); err != nil {
					select {
					case errors <- err:
					case <-ctx.Done():
						return
					}
				}

				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errors)
	}()

	return errors
}

// FanIn merges multiple input channels into a single output channel
func FanIn[T any](ctx context.Context, channels ...<-chan T) <-chan T {
	output := make(chan T)
	var wg sync.WaitGroup

	// Start a goroutine for each input channel
	for _, ch := range channels {
		wg.Add(1)
		go func(c <-chan T) {
			defer wg.Done()
			for item := range c {
				select {
				case output <- item:
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}

	// Close output when all inputs are drained
	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}

// Transform applies a transformation function to items in a channel
func Transform[T any, R any](ctx context.Context, input <-chan T, workers int, transform func(T) (R, error)) (<-chan R, <-chan error) {
	output := make(chan R, workers)
	errors := make(chan error, 10)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range input {
				result, err := transform(item)
				if err != nil {
					select {
					case errors <- err:
					case <-ctx.Done():
						return
					}
					continue
				}

				select {
				case output <- result:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(output)
		close(errors)
	}()

	return output, errors
}

// Filter filters items from a channel based on a predicate
func Filter[T any](ctx context.Context, input <-chan T, predicate func(T) bool) <-chan T {
	output := make(chan T)

	go func() {
		defer close(output)
		for item := range input {
			if predicate(item) {
				select {
				case output <- item:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output
}
