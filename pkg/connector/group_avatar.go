package connector

import (
	"bytes"
	"context"
	"hash/fnv"
	"image"
	"image/color"
	"image/png"
	"sort"
	"strings"

	_ "image/gif"
	_ "image/jpeg"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const groupAvatarSize = 128

type groupAvatarMember struct {
	MID         string
	Name        string
	PicturePath string
}

func (lc *LineClient) generatedGroupAvatar(ctx context.Context, chatMid string, members []string) *bridgev2.Avatar {
	avatarMembers := lc.groupAvatarMembers(ctx, members)
	if len(avatarMembers) == 0 {
		return nil
	}

	parts := make([]string, 0, len(avatarMembers)+1)
	parts = append(parts, chatMid)
	for _, member := range avatarMembers {
		parts = append(parts, member.MID+"="+member.PicturePath)
	}
	avatarID := networkid.AvatarID("line-group-composite:" + strings.Join(parts, "|"))

	return &bridgev2.Avatar{
		ID: avatarID,
		Get: func(ctx context.Context) ([]byte, error) {
			return lc.renderGeneratedGroupAvatar(ctx, avatarMembers)
		},
	}
}

func (lc *LineClient) groupAvatarMembers(ctx context.Context, members []string) []groupAvatarMember {
	seen := make(map[string]struct{}, len(members))
	candidates := make([]string, 0, len(members))
	for _, mid := range members {
		if mid == "" || mid == lc.Mid || mid == string(lc.UserLogin.ID) || strings.HasPrefix(mid, "c") || strings.HasPrefix(mid, "r") {
			continue
		}
		if _, ok := seen[mid]; ok {
			continue
		}
		seen[mid] = struct{}{}
		candidates = append(candidates, mid)
	}
	sort.Strings(candidates)

	avatarMembers := make([]groupAvatarMember, 0, 4)
	for _, mid := range candidates {
		contact := lc.getContact(ctx, mid)
		avatarMembers = append(avatarMembers, groupAvatarMember{
			MID:         mid,
			Name:        contact.EffectiveDisplayName(),
			PicturePath: contact.PicturePath,
		})
		if len(avatarMembers) == 4 {
			break
		}
	}
	return avatarMembers
}

func (lc *LineClient) renderGeneratedGroupAvatar(ctx context.Context, members []groupAvatarMember) ([]byte, error) {
	dst := image.NewRGBA(image.Rect(0, 0, groupAvatarSize, groupAvatarSize))
	fill(dst, color.RGBA{R: 232, G: 237, B: 243, A: 255})

	for i, member := range members {
		centerX, centerY, radius := groupAvatarSlot(i, len(members))
		img, err := lc.memberAvatarImage(ctx, member)
		if err != nil {
			img = nil
		}
		drawCircularAvatar(dst, img, centerX, centerY, radius, fallbackAvatarColor(member))
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, dst); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (lc *LineClient) memberAvatarImage(ctx context.Context, member groupAvatarMember) (image.Image, error) {
	if member.PicturePath == "" {
		return nil, nil
	}
	data, err := lc.GetAvatar(ctx, networkid.AvatarID(member.PicturePath))
	if err != nil {
		return nil, err
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return img, nil
}

func groupAvatarSlot(index, total int) (centerX, centerY, radius int) {
	switch total {
	case 1:
		return 64, 64, 58
	case 2:
		if index == 0 {
			return 43, 64, 42
		}
		return 85, 64, 42
	case 3:
		switch index {
		case 0:
			return 64, 41, 38
		case 1:
			return 43, 87, 38
		default:
			return 85, 87, 38
		}
	default:
		if index%2 == 0 {
			centerX = 42
		} else {
			centerX = 86
		}
		if index < 2 {
			centerY = 42
		} else {
			centerY = 86
		}
		return centerX, centerY, 34
	}
}

func drawCircularAvatar(dst *image.RGBA, src image.Image, centerX, centerY, radius int, fallback color.RGBA) {
	srcBounds := image.Rectangle{}
	if src != nil {
		srcBounds = squareBounds(src.Bounds())
	}

	radiusSquared := radius * radius
	for y := centerY - radius; y <= centerY+radius; y++ {
		for x := centerX - radius; x <= centerX+radius; x++ {
			if x < 0 || y < 0 || x >= groupAvatarSize || y >= groupAvatarSize {
				continue
			}
			dx := x - centerX
			dy := y - centerY
			if dx*dx+dy*dy > radiusSquared {
				continue
			}
			if src == nil {
				dst.SetRGBA(x, y, fallback)
				continue
			}
			srcX := srcBounds.Min.X + ((x - (centerX - radius)) * srcBounds.Dx() / (radius * 2))
			srcY := srcBounds.Min.Y + ((y - (centerY - radius)) * srcBounds.Dy() / (radius * 2))
			dst.Set(x, y, src.At(srcX, srcY))
		}
	}
}

func squareBounds(bounds image.Rectangle) image.Rectangle {
	size := bounds.Dx()
	if bounds.Dy() < size {
		size = bounds.Dy()
	}
	x0 := bounds.Min.X + (bounds.Dx()-size)/2
	y0 := bounds.Min.Y + (bounds.Dy()-size)/2
	return image.Rect(x0, y0, x0+size, y0+size)
}

func fill(img *image.RGBA, c color.RGBA) {
	for y := 0; y < groupAvatarSize; y++ {
		for x := 0; x < groupAvatarSize; x++ {
			img.SetRGBA(x, y, c)
		}
	}
}

func fallbackAvatarColor(member groupAvatarMember) color.RGBA {
	h := fnv.New32a()
	_, _ = h.Write([]byte(member.MID + member.Name))
	palette := []color.RGBA{
		{R: 77, G: 123, B: 223, A: 255},
		{R: 220, G: 82, B: 112, A: 255},
		{R: 52, G: 154, B: 120, A: 255},
		{R: 222, G: 145, B: 64, A: 255},
		{R: 128, G: 96, B: 194, A: 255},
		{R: 76, G: 147, B: 190, A: 255},
	}
	return palette[int(h.Sum32())%len(palette)]
}
