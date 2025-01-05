package clock

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/clock/event"
	"github.com/storacha/go-pail/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestAdvance(t *testing.T) {
	ctx := context.Background()
	stringBinder := testutil.NewStringBinder(t)
	testEventBinder := testutil.NewTestEventBinder(t)

	t.Run("create a new clock", func(t *testing.T) {
		bs := testutil.NewBlockstore()
		e := event.NewEvent("test", nil)

		b, err := event.MarshalBlock(e, stringBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b)
		require.NoError(t, err)

		head, err := Advance(ctx, bs, stringBinder, []ipld.Link{}, b.Link())
		require.NoError(t, err)

		for line, err := range Visualize(ctx, bs, stringBinder, head) {
			require.NoError(t, err)
			fmt.Println(line)
		}
		require.Len(t, head, 1)
		require.Contains(t, head, b.Link())
	})

	t.Run("add an event", func(t *testing.T) {
		bs := testutil.NewBlockstore()

		e0 := event.NewEvent(testutil.RandomEventData(t), nil)
		b0, err := event.MarshalBlock(e0, testEventBinder)
		require.NoError(t, err)

		head := []ipld.Link{b0.Link()}

		e1 := event.NewEvent(testutil.RandomEventData(t), head)
		b1, err := event.MarshalBlock(e1, testEventBinder)
		require.NoError(t, err)

		err = bs.PutAll(ctx, b0, b1)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b1.Link())
		require.NoError(t, err)

		for line, err := range Visualize(ctx, bs, testEventBinder, head) {
			require.NoError(t, err)
			fmt.Println(line)
		}
		require.Len(t, head, 1)
		require.Contains(t, head, b1.Link())
	})

	t.Run("add two events with shared parents", func(t *testing.T) {
		bs := testutil.NewBlockstore()

		e0 := event.NewEvent(testutil.RandomEventData(t), nil)
		b0, err := event.MarshalBlock(e0, testEventBinder)
		require.NoError(t, err)

		head := []ipld.Link{b0.Link()}
		parents := head

		e1 := event.NewEvent(testutil.RandomEventData(t), parents)
		b1, err := event.MarshalBlock(e1, testEventBinder)
		require.NoError(t, err)

		err = bs.PutAll(ctx, b0, b1)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, parents, b1.Link())
		require.NoError(t, err)

		e2 := event.NewEvent(testutil.RandomEventData(t), parents)
		b2, err := event.MarshalBlock(e2, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b2)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b2.Link())
		require.NoError(t, err)

		for line, err := range Visualize(ctx, bs, testEventBinder, head) {
			require.NoError(t, err)
			fmt.Println(line)
		}
		require.Len(t, head, 2)
		require.Contains(t, head, b1.Link())
		require.Contains(t, head, b2.Link())
	})

	t.Run("add two events with some shared parents", func(t *testing.T) {
		bs := testutil.NewBlockstore()

		e0 := event.NewEvent(testutil.RandomEventData(t), nil)
		b0, err := event.MarshalBlock(e0, testEventBinder)
		require.NoError(t, err)

		head := []ipld.Link{b0.Link()}
		parents := head

		e1 := event.NewEvent(testutil.RandomEventData(t), parents)
		b1, err := event.MarshalBlock(e1, testEventBinder)
		require.NoError(t, err)

		err = bs.PutAll(ctx, b0, b1)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, parents, b1.Link())
		require.NoError(t, err)

		e2 := event.NewEvent(testutil.RandomEventData(t), parents)
		b2, err := event.MarshalBlock(e2, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b2)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b2.Link())
		require.NoError(t, err)

		e3 := event.NewEvent(testutil.RandomEventData(t), parents)
		b3, err := event.MarshalBlock(e3, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b3)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b3.Link())
		require.NoError(t, err)

		e4 := event.NewEvent(testutil.RandomEventData(t), []ipld.Link{b1.Link(), b2.Link()})
		b4, err := event.MarshalBlock(e4, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b4)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b4.Link())
		require.NoError(t, err)

		e5 := event.NewEvent(testutil.RandomEventData(t), []ipld.Link{b3.Link()})
		b5, err := event.MarshalBlock(e5, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b5)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b5.Link())
		require.NoError(t, err)

		for line, err := range Visualize(ctx, bs, testEventBinder, head) {
			require.NoError(t, err)
			fmt.Println(line)
		}
		require.Len(t, head, 2)
		require.Contains(t, head, b4.Link())
		require.Contains(t, head, b5.Link())
	})

	t.Run("converge when multi-root", func(t *testing.T) {
		bs := testutil.NewBlockstore()

		e0 := event.NewEvent(testutil.RandomEventData(t), nil)
		b0, err := event.MarshalBlock(e0, testEventBinder)
		require.NoError(t, err)

		head := []ipld.Link{b0.Link()}
		parents0 := head

		e1 := event.NewEvent(testutil.RandomEventData(t), parents0)
		b1, err := event.MarshalBlock(e1, testEventBinder)
		require.NoError(t, err)

		err = bs.PutAll(ctx, b0, b1)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, parents0, b1.Link())
		require.NoError(t, err)

		e2 := event.NewEvent(testutil.RandomEventData(t), parents0)
		b2, err := event.MarshalBlock(e2, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b2)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b2.Link())
		require.NoError(t, err)

		parents1 := head

		e3 := event.NewEvent(testutil.RandomEventData(t), parents1)
		b3, err := event.MarshalBlock(e3, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b3)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b3.Link())
		require.NoError(t, err)

		e4 := event.NewEvent(testutil.RandomEventData(t), parents1)
		b4, err := event.MarshalBlock(e4, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b4)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b4.Link())
		require.NoError(t, err)

		e5 := event.NewEvent(testutil.RandomEventData(t), parents1)
		b5, err := event.MarshalBlock(e5, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b5)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b5.Link())
		require.NoError(t, err)

		parents2 := head

		e6 := event.NewEvent(testutil.RandomEventData(t), parents2)
		b6, err := event.MarshalBlock(e6, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b6)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b6.Link())
		require.NoError(t, err)

		for line, err := range Visualize(ctx, bs, testEventBinder, head) {
			require.NoError(t, err)
			fmt.Println(line)
		}
		require.Len(t, head, 1)
		require.Contains(t, head, b6.Link())
	})

	t.Run("add an old event", func(t *testing.T) {
		bs := testutil.NewBlockstore()

		e0 := event.NewEvent(testutil.RandomEventData(t), nil)
		b0, err := event.MarshalBlock(e0, testEventBinder)
		require.NoError(t, err)

		head := []ipld.Link{b0.Link()}
		parents0 := head

		e1 := event.NewEvent(testutil.RandomEventData(t), parents0)
		b1, err := event.MarshalBlock(e1, testEventBinder)
		require.NoError(t, err)

		err = bs.PutAll(ctx, b0, b1)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, parents0, b1.Link())
		require.NoError(t, err)

		e2 := event.NewEvent(testutil.RandomEventData(t), parents0)
		b2, err := event.MarshalBlock(e2, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b2)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b2.Link())
		require.NoError(t, err)

		parents1 := head

		e3 := event.NewEvent(testutil.RandomEventData(t), parents1)
		b3, err := event.MarshalBlock(e3, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b3)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b3.Link())
		require.NoError(t, err)

		e4 := event.NewEvent(testutil.RandomEventData(t), parents1)
		b4, err := event.MarshalBlock(e4, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b4)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b4.Link())
		require.NoError(t, err)

		e5 := event.NewEvent(testutil.RandomEventData(t), parents1)
		b5, err := event.MarshalBlock(e5, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b5)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b5.Link())
		require.NoError(t, err)

		parents2 := head

		e6 := event.NewEvent(testutil.RandomEventData(t), parents2)
		b6, err := event.MarshalBlock(e6, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b6)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b6.Link())
		require.NoError(t, err)

		// now very old one
		e7 := event.NewEvent(testutil.RandomEventData(t), parents0)
		b7, err := event.MarshalBlock(e7, testEventBinder)
		require.NoError(t, err)

		err = bs.Put(ctx, b7)
		require.NoError(t, err)

		before := bs.GetCount
		head, err = Advance(ctx, bs, testEventBinder, head, b7.Link())
		require.NoError(t, err)
		require.Equal(t, 10, bs.GetCount-before)

		for line, err := range Visualize(ctx, bs, testEventBinder, head) {
			require.NoError(t, err)
			fmt.Println(line)
		}
		require.Len(t, head, 2)
		require.Contains(t, head, b6.Link())
		require.Contains(t, head, b7.Link())
	})

	t.Run("add an event with missing parents", func(t *testing.T) {
		bs := testutil.NewBlockstore()

		e0 := event.NewEvent(testutil.RandomEventData(t), nil)
		b0, err := event.MarshalBlock(e0, testEventBinder)
		require.NoError(t, err)

		head := []ipld.Link{b0.Link()}

		e1 := event.NewEvent(testutil.RandomEventData(t), head)
		b1, err := event.MarshalBlock(e1, testEventBinder)
		require.NoError(t, err)

		e2 := event.NewEvent(testutil.RandomEventData(t), []ipld.Link{b1.Link()})
		b2, err := event.MarshalBlock(e2, testEventBinder)
		require.NoError(t, err)

		err = bs.PutAll(ctx, b0, b1, b2)
		require.NoError(t, err)

		head, err = Advance(ctx, bs, testEventBinder, head, b2.Link())
		require.NoError(t, err)

		for line, err := range Visualize(ctx, bs, testEventBinder, head) {
			require.NoError(t, err)
			fmt.Println(line)
		}
		require.Len(t, head, 1)
		require.Contains(t, head, b2.Link())
	})
}
