package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/util/variationselector"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const (
	lineOriginalEmojiProductID = "670e0cce840a8236ddd4ee4c"
	lineTrialEmojiProductID    = "5ac1bfd5040ab15980c9b435"
	maxLineReqSeq              = 1_000_000_000
	sentReqSeqTTL              = 5 * time.Minute
)

type linePaidReactionRef struct {
	ProductID    string
	EmojiID      string
	ResourceType int
	Version      int
}

type lineReactionRef struct {
	typ line.ReactionType
}

type ReactionMetadata struct {
	MatrixKey    string            `json:"matrix_key,omitempty"`
	ReactionType line.ReactionType `json:"reaction_type"`
}

func (ref linePaidReactionRef) reactionType() line.ReactionType {
	return line.ReactionType{
		PaidReactionType: &line.PaidReactionType{
			ProductID:    ref.ProductID,
			EmojiID:      ref.EmojiID,
			ResourceType: ref.ResourceType,
			Version:      ref.Version,
		},
	}
}

func cloneLineReactionType(typ line.ReactionType) line.ReactionType {
	cloned := line.ReactionType{
		PredefinedReactionType: typ.PredefinedReactionType,
	}
	if typ.PaidReactionType != nil {
		paid := *typ.PaidReactionType
		cloned.PaidReactionType = &paid
	}
	return cloned
}

func newLineReactionRef(typ line.ReactionType) (lineReactionRef, error) {
	hasPredefined := typ.PredefinedReactionType != 0
	hasPaid := typ.PaidReactionType != nil
	if hasPredefined == hasPaid {
		return lineReactionRef{}, errors.New("reaction type must contain exactly one predefined or paid reaction")
	}
	if hasPredefined {
		if _, ok := line.PredefinedReactionEmoji[typ.PredefinedReactionType]; !ok {
			return lineReactionRef{}, fmt.Errorf("unknown predefined reaction type %d", typ.PredefinedReactionType)
		}
	} else if typ.PaidReactionType.ProductID == "" || typ.PaidReactionType.EmojiID == "" {
		return lineReactionRef{}, errors.New("paid reaction is missing product or emoji ID")
	}
	return lineReactionRef{typ: cloneLineReactionType(typ)}, nil
}

func (ref lineReactionRef) reactionType() line.ReactionType {
	return cloneLineReactionType(ref.typ)
}

func (ref lineReactionRef) networkEmojiID() networkid.EmojiID {
	if ref.typ.PaidReactionType != nil {
		return networkid.EmojiID("paid:" + ref.typ.PaidReactionType.ProductID + ":" + ref.typ.PaidReactionType.EmojiID)
	}
	return networkid.EmojiID("predefined:" + strconv.Itoa(ref.typ.PredefinedReactionType))
}

func (ref lineReactionRef) equal(other lineReactionRef) bool {
	if ref.typ.PredefinedReactionType != other.typ.PredefinedReactionType {
		return false
	}
	if ref.typ.PaidReactionType == nil || other.typ.PaidReactionType == nil {
		return ref.typ.PaidReactionType == nil && other.typ.PaidReactionType == nil
	}
	return *ref.typ.PaidReactionType == *other.typ.PaidReactionType
}

func (ref lineReactionRef) metadata(matrixKey string) *ReactionMetadata {
	return &ReactionMetadata{
		MatrixKey:    matrixKey,
		ReactionType: ref.reactionType(),
	}
}

// These are the LINE emoji/sticon URLs from the issue's pack-based reaction
// set. Add more entries here as more Matrix emoji -> LINE CDN URL mappings are
// captured.
var lineEmojiReactionURLs = map[string]string{
	"\U0001F40D": lineSticonURL(lineOriginalEmojiProductID, "064"),
	"\U0001F43C": lineSticonURL(lineOriginalEmojiProductID, "068"),
	"\U0001F642": lineSticonURL(lineOriginalEmojiProductID, "077"),
	"\U0001F60A": lineSticonURL(lineOriginalEmojiProductID, "078"),
	"\U0001F604": lineSticonURL(lineOriginalEmojiProductID, "079"),
	"\U0001F60D": lineSticonURL(lineTrialEmojiProductID, "001"),
	"\U0001F606": lineSticonURL(lineTrialEmojiProductID, "002"),
	"\U0001F60C": lineSticonURL(lineTrialEmojiProductID, "012"),
	"\U0001F602": lineSticonURL(lineOriginalEmojiProductID, "080"),
	"\U0001F979": lineSticonURL(lineOriginalEmojiProductID, "081"),
	"\U0001F632": lineSticonURL(lineTrialEmojiProductID, "029"),
	"\U0001F611": lineSticonURL(lineTrialEmojiProductID, "036"),
	"\U0001F61A": lineSticonURL(lineOriginalEmojiProductID, "082"),
	"\U0001F607": lineSticonURL(lineOriginalEmojiProductID, "083"),
	"\U0001F970": lineSticonURL(lineOriginalEmojiProductID, "084"),
	"\U0001F609": lineSticonURL(lineTrialEmojiProductID, "011"),
	"\U0001F61D": lineSticonURL(lineOriginalEmojiProductID, "085"),
	"\U0001F60E": lineSticonURL(lineOriginalEmojiProductID, "086"),
	"\U0001F97A": lineSticonURL(lineOriginalEmojiProductID, "087"),
	"\U0001F641": lineSticonURL(lineOriginalEmojiProductID, "088"),
	"\U0001F62E": lineSticonURL(lineOriginalEmojiProductID, "089"),
	"\U0001F627": lineSticonURL(lineOriginalEmojiProductID, "090"),
	"\U0001F622": lineSticonURL(lineOriginalEmojiProductID, "092"),
	"\U0001F62D": lineSticonURL(lineOriginalEmojiProductID, "093"),
	"\U0001F620": lineSticonURL(lineOriginalEmojiProductID, "094"),
	"\U0001F635": lineSticonURL(lineOriginalEmojiProductID, "095"),
	"\U0001F616": lineSticonURL(lineTrialEmojiProductID, "129"),
	"\U0001F624": lineSticonURL(lineTrialEmojiProductID, "135"),
	"\U0001F613": lineSticonURL(lineOriginalEmojiProductID, "097"),
	"\U0001F60F": lineSticonURL(lineOriginalEmojiProductID, "098"),
	"\U0001F612": lineSticonURL(lineTrialEmojiProductID, "141"),
	"\U0001FAE8": lineSticonURL(lineTrialEmojiProductID, "142"),
	"\U0001F978": lineSticonURL(lineTrialEmojiProductID, "146"),
	"\U0001F605": lineSticonURL(lineOriginalEmojiProductID, "099"),
	"\U0001F633": lineSticonURL(lineOriginalEmojiProductID, "100"),
	"\U0001F631": lineSticonURL(lineOriginalEmojiProductID, "101"),
	"\U0001F972": lineSticonURL(lineOriginalEmojiProductID, "102"),
	"\U0001F62A": lineSticonURL(lineOriginalEmojiProductID, "103"),
	"\U0001F924": lineSticonURL(lineOriginalEmojiProductID, "104"),
	"\U0001F971": lineSticonURL(lineOriginalEmojiProductID, "105"),
	"\U0001F92E": lineSticonURL(lineOriginalEmojiProductID, "107"),
	"\U0001F637": lineSticonURL(lineOriginalEmojiProductID, "108"),
	"\U0001F621": lineSticonURL(lineOriginalEmojiProductID, "109"),
	"\U0001F608": lineSticonURL(lineOriginalEmojiProductID, "110"),
	"\U0001F914": lineSticonURL(lineOriginalEmojiProductID, "118"),
	"\U0001FAE0": lineSticonURL(lineOriginalEmojiProductID, "125"),
	"\U0001F44D": lineSticonURL(lineOriginalEmojiProductID, "143"),
	"\U0001F44E": lineSticonURL(lineOriginalEmojiProductID, "144"),
	"\U0001F91E": lineSticonURL(lineOriginalEmojiProductID, "145"),
	"\u270C":     lineSticonURL(lineOriginalEmojiProductID, "146"),
	"\U0001F442": lineSticonURL(lineTrialEmojiProductID, "246"),
	"\U0001F443": lineSticonURL(lineTrialEmojiProductID, "245"),
	"\U0001F444": lineSticonURL(lineTrialEmojiProductID, "247"),
	"\U0001F44B": lineSticonURL(lineOriginalEmojiProductID, "147"),
	"\U0001F64F": lineSticonURL(lineOriginalEmojiProductID, "148"),
	"\U0001F4AA": lineSticonURL(lineOriginalEmojiProductID, "149"),
	"\U0001FAF6": lineSticonURL(lineOriginalEmojiProductID, "150"),
	"\U0001F448": lineSticonURL(lineOriginalEmojiProductID, "151"),
	"\U0001F449": lineSticonURL(lineOriginalEmojiProductID, "152"),
	"\U0001F918": lineSticonURL(lineOriginalEmojiProductID, "153"),
	"\U0001F44C": lineSticonURL(lineOriginalEmojiProductID, "154"),
	"\U0001F44A": lineSticonURL(lineOriginalEmojiProductID, "155"),
	"\U0001FAF0": lineSticonURL(lineOriginalEmojiProductID, "156"),
	"\U0001F431": lineSticonURL(lineOriginalEmojiProductID, "157"),
	"\U0001F436": lineSticonURL(lineOriginalEmojiProductID, "158"),
	"\U0001F385": lineSticonURL(lineOriginalEmojiProductID, "159"),
	"\U0001F47B": lineSticonURL(lineOriginalEmojiProductID, "160"),
	"\U0001F921": lineSticonURL(lineOriginalEmojiProductID, "161"),
	"\U0001F47D": lineSticonURL(lineOriginalEmojiProductID, "162"),
	"\U0001F4A9": lineSticonURL(lineOriginalEmojiProductID, "163"),
	"\U0001F4B0": lineSticonURL(lineOriginalEmojiProductID, "164"),
	"\u2764":     lineSticonURL(lineOriginalEmojiProductID, "165"),
	"\U0001F494": lineSticonURL(lineOriginalEmojiProductID, "166"),
	"\U0001F495": lineSticonURL(lineTrialEmojiProductID, "224"),
	"\U0001F496": lineSticonURL(lineTrialEmojiProductID, "225"),
	"\U0001F497": lineSticonURL(lineTrialEmojiProductID, "226"),
	"\U0001F498": lineSticonURL(lineTrialEmojiProductID, "227"),
	"\U0001F525": lineSticonURL(lineOriginalEmojiProductID, "167"),
	"\u2728":     lineSticonURL(lineOriginalEmojiProductID, "168"),
	"\U0001F4A6": lineSticonURL(lineOriginalEmojiProductID, "169"),
	"\U0001F3B5": lineSticonURL(lineOriginalEmojiProductID, "170"),
	"\U0001F3B6": lineSticonURL(lineOriginalEmojiProductID, "171"),
	"\U0001F389": lineSticonURL(lineOriginalEmojiProductID, "172"),
	"\U0001F34E": lineSticonURL(lineOriginalEmojiProductID, "173"),
	"\U0001F34C": lineSticonURL(lineOriginalEmojiProductID, "174"),
	"\U0001F966": lineSticonURL(lineOriginalEmojiProductID, "175"),
	"\U0001F35E": lineSticonURL(lineOriginalEmojiProductID, "176"),
	"\U0001F356": lineSticonURL(lineOriginalEmojiProductID, "177"),
	"\U0001F354": lineSticonURL(lineOriginalEmojiProductID, "178"),
	"\U0001F366": lineSticonURL(lineOriginalEmojiProductID, "179"),
	"\U0001F382": lineSticonURL(lineOriginalEmojiProductID, "180"),
	"\u2615":     lineSticonURL(lineOriginalEmojiProductID, "181"),
	"\U0001F964": lineSticonURL(lineOriginalEmojiProductID, "182"),
	"\U0001F37A": lineSticonURL(lineOriginalEmojiProductID, "183"),
	"\u2600":     lineSticonURL(lineOriginalEmojiProductID, "184"),
	"\u2B50":     lineSticonURL(lineOriginalEmojiProductID, "185"),
	"\U0001F319": lineSticonURL(lineOriginalEmojiProductID, "186"),
	"\U0001F338": lineSticonURL(lineOriginalEmojiProductID, "187"),
	"\U0001FAB4": lineSticonURL(lineOriginalEmojiProductID, "188"),
	"\U0001F332": lineSticonURL(lineOriginalEmojiProductID, "189"),
	"\U0001F30A": lineSticonURL(lineOriginalEmojiProductID, "190"),
	"\u26F0":     lineSticonURL(lineOriginalEmojiProductID, "191"),
	"\U0001F30D": lineSticonURL(lineOriginalEmojiProductID, "192"),
	"\U0001F697": lineSticonURL(lineOriginalEmojiProductID, "193"),
	"\U0001F691": lineSticonURL(lineOriginalEmojiProductID, "194"),
	"\u26BD":     lineSticonURL(lineOriginalEmojiProductID, "195"),
	"\U0001F3A4": lineSticonURL(lineOriginalEmojiProductID, "196"),
	"\U0001F3B8": lineSticonURL(lineOriginalEmojiProductID, "197"),
	"\U0001F6E0": lineSticonURL(lineOriginalEmojiProductID, "198"),
	"\U0001F552": lineSticonURL(lineOriginalEmojiProductID, "199"),
	"\u2705":     lineSticonURL(lineOriginalEmojiProductID, "200"),
	"\u274C":     lineSticonURL(lineOriginalEmojiProductID, "201"),
	"0":          lineSticonURL(lineOriginalEmojiProductID, "202"),
	"1":          lineSticonURL(lineOriginalEmojiProductID, "203"),
	"2":          lineSticonURL(lineOriginalEmojiProductID, "204"),
	"3":          lineSticonURL(lineOriginalEmojiProductID, "205"),
	"4":          lineSticonURL(lineOriginalEmojiProductID, "206"),
	"5":          lineSticonURL(lineOriginalEmojiProductID, "207"),
	"6":          lineSticonURL(lineOriginalEmojiProductID, "208"),
	"7":          lineSticonURL(lineOriginalEmojiProductID, "209"),
	"8":          lineSticonURL(lineOriginalEmojiProductID, "210"),
	"9":          lineSticonURL(lineOriginalEmojiProductID, "211"),
}

// lineAllowedReactions is the Matrix-facing form of the outbound LINE reaction
// map. Room capability hashes depend on slice order, so keep this list sorted.
var lineAllowedReactions = func() []string {
	reactions := make([]string, 0, len(lineEmojiReactionURLs))
	for reaction := range lineEmojiReactionURLs {
		// The send lookup normalizes keycaps to bare digits. Restore their emoji
		// form before advertising them to clients.
		if len(reaction) == 1 && reaction[0] >= '0' && reaction[0] <= '9' {
			reaction += "\u20E3"
		}
		reactions = append(reactions, variationselector.Add(reaction))
	}
	slices.Sort(reactions)
	return reactions
}()

func getLineAllowedReactions() []string {
	return slices.Clone(lineAllowedReactions)
}

func lineSticonURL(productID, emojiID string) string {
	return fmt.Sprintf("https://stickershop.line-scdn.net/sticonshop/v1/sticon/%s/android/%s.png", productID, emojiID)
}

func reactionUploadMXC(uploadedMXC string, uploadedFile *event.EncryptedFileInfo) (string, error) {
	if uploadedFile != nil {
		return "", errors.New("reaction icon upload returned encrypted media")
	}
	if uploadedMXC == "" {
		return "", errors.New("reaction icon upload returned an empty MXC URI")
	}
	return uploadedMXC, nil
}

func (lc *LineClient) getPredefinedReactionMXC(ctx context.Context, prt int) (string, error) {
	if _, ok := line.PredefinedReactionEmoji[prt]; !ok {
		return "", fmt.Errorf("unknown predefined reaction type %d", prt)
	}

	lc.cacheMu.Lock()
	mxc := lc.reactionIconMXC[prt]
	lc.cacheMu.Unlock()
	if mxc != "" {
		return mxc, nil
	}

	pngData, err := getReactionIconData(prt)
	if err != nil {
		return "", fmt.Errorf("get reaction icon data: %w", err)
	}
	uploadedMXC, uploadedFile, err := lc.UserLogin.Bridge.Bot.UploadMedia(ctx, "", pngData, "reaction.png", "image/png")
	if err != nil {
		return "", fmt.Errorf("upload reaction icon: %w", err)
	}
	mxc, err = reactionUploadMXC(string(uploadedMXC), uploadedFile)
	if err != nil {
		return "", err
	}

	lc.cacheMu.Lock()
	if lc.reactionIconMXC == nil {
		lc.reactionIconMXC = make(map[int]string)
	}
	if cached := lc.reactionIconMXC[prt]; cached != "" {
		mxc = cached
	} else {
		lc.reactionIconMXC[prt] = mxc
	}
	lc.cacheMu.Unlock()
	return mxc, nil
}

func (lc *LineClient) getPaidReactionMXC(ctx context.Context, prt *line.PaidReactionType) (string, error) {
	if prt == nil || prt.ProductID == "" || prt.EmojiID == "" {
		return "", errors.New("paid reaction is missing product or emoji ID")
	}
	iconURL := lineSticonURL(prt.ProductID, prt.EmojiID)

	lc.cacheMu.Lock()
	mxc := lc.paidReactionIconMXC[iconURL]
	lc.cacheMu.Unlock()
	if mxc != "" {
		return mxc, nil
	}

	resp, err := lc.HTTPClient.Get(iconURL)
	if err != nil {
		return "", fmt.Errorf("download paid reaction icon: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("download paid reaction icon: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read paid reaction icon: %w", err)
	}
	mimeType := resp.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = "image/png"
	}
	uploadedMXC, uploadedFile, err := lc.UserLogin.Bridge.Bot.UploadMedia(ctx, "", data, "reaction.png", mimeType)
	if err != nil {
		return "", fmt.Errorf("upload paid reaction icon: %w", err)
	}
	mxc, err = reactionUploadMXC(string(uploadedMXC), uploadedFile)
	if err != nil {
		return "", fmt.Errorf("paid %w", err)
	}

	lc.cacheMu.Lock()
	if lc.paidReactionIconMXC == nil {
		lc.paidReactionIconMXC = make(map[string]string)
	}
	if cached := lc.paidReactionIconMXC[iconURL]; cached != "" {
		mxc = cached
	} else {
		lc.paidReactionIconMXC[iconURL] = mxc
	}
	lc.cacheMu.Unlock()
	return mxc, nil
}

func (lc *LineClient) convertReaction(
	ctx context.Context,
	typ line.ReactionType,
	sender bridgev2.EventSender,
	timestamp time.Time,
) (*bridgev2.BackfillReaction, error) {
	ref, err := newLineReactionRef(typ)
	if err != nil {
		return nil, err
	}

	var mxc string
	if ref.typ.PaidReactionType != nil {
		mxc, err = lc.getPaidReactionMXC(ctx, ref.typ.PaidReactionType)
	} else {
		mxc, err = lc.getPredefinedReactionMXC(ctx, ref.typ.PredefinedReactionType)
	}
	if err != nil {
		return nil, err
	}

	return &bridgev2.BackfillReaction{
		Timestamp:  timestamp,
		Sender:     sender,
		EmojiID:    ref.networkEmojiID(),
		Emoji:      mxc,
		DBMetadata: ref.metadata(mxc),
	}, nil
}

func (lc *LineClient) convertMessageReactions(ctx context.Context, msg *line.Message) ([]*bridgev2.BackfillReaction, bool) {
	if msg == nil || msg.Reactions == nil {
		return nil, false
	}

	converted := make([]*bridgev2.BackfillReaction, 0, len(msg.Reactions))
	complete := true
	for _, reaction := range msg.Reactions {
		if !isUserMID(reaction.FromUserMID) {
			complete = false
			lc.UserLogin.Bridge.Log.Warn().
				Str("msg_id", msg.ID).
				Str("reaction_sender", reaction.FromUserMID).
				Msg("Skipping historical reaction without a valid sender MID")
			continue
		}

		var timestamp time.Time
		if timestampMillis, err := reaction.AtMillis.Int64(); err == nil && timestampMillis > 0 {
			timestamp = time.UnixMilli(timestampMillis)
		}
		convertedReaction, err := lc.convertReaction(
			ctx,
			reaction.ReactionType,
			lc.eventSenderForMID(reaction.FromUserMID),
			timestamp,
		)
		if err != nil {
			complete = false
			lc.UserLogin.Bridge.Log.Warn().
				Err(err).
				Str("msg_id", msg.ID).
				Str("reaction_sender", reaction.FromUserMID).
				Msg("Skipping unsupported historical reaction")
			continue
		}
		converted = append(converted, convertedReaction)
	}
	return converted, complete
}

func (lc *LineClient) queueMessageReactionSync(ctx context.Context, chatMID string, msg *line.Message) bool {
	if msg == nil || ContentType(msg.ContentType) == ContentSystem || msg.Reactions == nil {
		return false
	}

	converted, complete := lc.convertMessageReactions(ctx, msg)
	if !complete {
		// The embedded list is authoritative, but an upload/parse failure
		// means our converted view is incomplete. Do not accidentally redact
		// reactions that LINE still reports.
		return false
	}
	users := make(map[networkid.UserID]*bridgev2.ReactionSyncUser, len(converted))
	var timestamp time.Time
	for _, reaction := range converted {
		if reaction.Timestamp.After(timestamp) {
			timestamp = reaction.Timestamp
		}
		// LINE allows one reaction per user per message. Keeping the last
		// record also makes malformed duplicate entries deterministic.
		users[reaction.Sender.Sender] = &bridgev2.ReactionSyncUser{
			Reactions:       []*bridgev2.BackfillReaction{reaction},
			HasAllReactions: true,
		}
	}
	if timestamp.IsZero() {
		timestamp = lc.parseMessageTimestamp(msg)
	}

	result := lc.UserLogin.Bridge.QueueRemoteEvent(lc.UserLogin, &simplevent.ReactionSync{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventReactionSync,
			PortalKey: networkid.PortalKey{ID: makePortalID(chatMID), Receiver: lc.UserLogin.ID},
			Timestamp: timestamp,
		},
		TargetMessage: networkid.MessageID(msg.ID),
		Reactions: &bridgev2.ReactionSyncData{
			Users:       users,
			HasAllUsers: true,
		},
	})
	return result.Success && !result.Ignored
}

func parseLineSticonURL(rawURL string) (linePaidReactionRef, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return linePaidReactionRef{}, err
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	for i := 0; i+5 < len(parts); i++ {
		if parts[i] != "sticonshop" || parts[i+1] != "v1" || parts[i+2] != "sticon" || parts[i+4] != "android" {
			continue
		}
		productID := parts[i+3]
		emojiFile := parts[i+5]
		emojiID := strings.TrimSuffix(emojiFile, ".png")
		if productID == "" || emojiID == "" || emojiID == emojiFile {
			break
		}
		version := 1
		if rawVersion := parsed.Query().Get("v"); rawVersion != "" {
			version, _ = strconv.Atoi(rawVersion)
		}
		return linePaidReactionRef{
			ProductID:    productID,
			EmojiID:      emojiID,
			ResourceType: 1,
			Version:      version,
		}, nil
	}
	return linePaidReactionRef{}, fmt.Errorf("not a LINE sticon URL: %s", rawURL)
}

func normalizeMatrixReactionKey(key string) string {
	key = strings.Map(func(r rune) rune {
		if r >= '\U0001F3FB' && r <= '\U0001F3FF' {
			return -1
		}
		switch r {
		case '\uFE0E', '\uFE0F':
			return -1
		default:
			return r
		}
	}, key)

	runes := []rune(key)
	if len(runes) == 2 && runes[1] == '\u20E3' && runes[0] >= '0' && runes[0] <= '9' {
		return string(runes[0])
	}
	return key
}

func linePaidReactionForMatrixEmoji(key string) (linePaidReactionRef, bool) {
	rawURL, ok := lineEmojiReactionURLs[normalizeMatrixReactionKey(key)]
	if !ok {
		return linePaidReactionRef{}, false
	}
	ref, err := parseLineSticonURL(rawURL)
	if err != nil {
		return linePaidReactionRef{}, false
	}
	return ref, true
}

func storedLineReactionForMatrixKey(key string, reactions []*database.Reaction) (lineReactionRef, bool) {
	var (
		found    lineReactionRef
		hasFound bool
	)
	for _, reaction := range reactions {
		meta, ok := reaction.Metadata.(*ReactionMetadata)
		if !ok || meta == nil || meta.MatrixKey != key {
			continue
		}
		ref, err := newLineReactionRef(meta.ReactionType)
		if err != nil || (reaction.EmojiID != "" && reaction.EmojiID != ref.networkEmojiID()) {
			continue
		}
		if hasFound && !found.equal(ref) {
			return lineReactionRef{}, false
		}
		found = ref
		hasFound = true
	}
	return found, hasFound
}

func (lc *LineClient) resolveMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (lineReactionRef, error) {
	key := msg.Content.RelatesTo.GetAnnotationKey()
	if paidRef, ok := linePaidReactionForMatrixEmoji(key); ok {
		ref, err := newLineReactionRef(paidRef.reactionType())
		if err != nil {
			return lineReactionRef{}, err
		}
		return ref, nil
	}
	if !strings.HasPrefix(key, "mxc://") {
		return lineReactionRef{}, unsupportedMatrixReactionError(key)
	}
	if msg.TargetMessage == nil || msg.Portal == nil || msg.Portal.Bridge == nil || msg.Portal.Bridge.DB == nil {
		return lineReactionRef{}, errors.New("reaction target database context is missing")
	}

	reactions, err := msg.Portal.Bridge.DB.Reaction.GetAllToMessagePart(
		ctx,
		msg.Portal.Receiver,
		msg.TargetMessage.ID,
		msg.TargetMessage.PartID,
	)
	if err != nil {
		return lineReactionRef{}, fmt.Errorf("get target message reactions: %w", err)
	}
	ref, ok := storedLineReactionForMatrixKey(key, reactions)
	if !ok {
		return lineReactionRef{}, unsupportedMatrixReactionError(key)
	}
	return ref, nil
}

func unsupportedMatrixReactionError(key string) error {
	return bridgev2.WrapErrorInStatus(fmt.Errorf("LINE does not support Matrix reaction %q", key)).
		WithStatus(event.MessageStatusFail).
		WithIsCertain(true).
		WithErrorAsMessage().
		WithErrorReason(event.MessageStatusUnsupported)
}

func invalidReactionTargetError(messageID string) error {
	return bridgev2.WrapErrorInStatus(fmt.Errorf("LINE reaction target message ID %q is invalid", messageID)).
		WithStatus(event.MessageStatusFail).
		WithIsCertain(true).
		WithErrorAsMessage().
		WithErrorReason(event.MessageStatusUnsupported)
}

func reactionNotAMemberError() error {
	return bridgev2.WrapErrorInStatus(fmt.Errorf("LINE says this account is not a member of the chat")).
		WithStatus(event.MessageStatusFail).
		WithIsCertain(true).
		WithErrorAsMessage().
		WithErrorReason(event.MessageStatusNoPermission)
}

func parseReactionTargetMessageID(messageID networkid.MessageID) (string, error) {
	raw := string(messageID)
	if raw == "" || strings.HasPrefix(raw, "local-") || strings.HasPrefix(raw, "$") {
		return "", invalidReactionTargetError(raw)
	}
	if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
		return "", invalidReactionTargetError(raw)
	}
	return raw, nil
}

func (lc *LineClient) nextReqSeq() int {
	return lc.nextReqSeqWithTracking(true)
}

func (lc *LineClient) nextUntrackedReqSeq() int {
	return lc.nextReqSeqWithTracking(false)
}

func (lc *LineClient) nextReqSeqWithTracking(track bool) int {
	now := time.Now()

	lc.reqSeqMu.Lock()
	defer lc.reqSeqMu.Unlock()

	lc.cleanupSentReqSeqsLocked(now)
	if lc.sentReqSeqs == nil {
		lc.sentReqSeqs = make(map[int]time.Time)
	}
	if lc.lastReqSeq <= 0 {
		lc.lastReqSeq = int(now.UnixMilli() % maxLineReqSeq)
	}

	for {
		lc.lastReqSeq++
		if lc.lastReqSeq <= 0 || lc.lastReqSeq >= maxLineReqSeq {
			lc.lastReqSeq = 1
		}
		if _, exists := lc.sentReqSeqs[lc.lastReqSeq]; !exists {
			if track {
				lc.sentReqSeqs[lc.lastReqSeq] = now
			}
			return lc.lastReqSeq
		}
	}
}

func (lc *LineClient) cleanupSentReqSeqsLocked(now time.Time) {
	for reqSeq, sentAt := range lc.sentReqSeqs {
		if now.Sub(sentAt) > sentReqSeqTTL {
			delete(lc.sentReqSeqs, reqSeq)
		}
	}
}

func (lc *LineClient) trackReqSeq(reqSeq int) {
	if reqSeq <= 0 {
		return
	}
	now := time.Now()

	lc.reqSeqMu.Lock()
	if lc.sentReqSeqs == nil {
		lc.sentReqSeqs = make(map[int]time.Time)
	}
	lc.cleanupSentReqSeqsLocked(now)
	lc.sentReqSeqs[reqSeq] = now
	if reqSeq > lc.lastReqSeq {
		lc.lastReqSeq = reqSeq
	}
	lc.reqSeqMu.Unlock()
}

func (lc *LineClient) consumeSentReqSeq(reqSeq int) bool {
	if reqSeq <= 0 {
		return false
	}
	now := time.Now()

	lc.reqSeqMu.Lock()
	lc.cleanupSentReqSeqsLocked(now)
	_, ok := lc.sentReqSeqs[reqSeq]
	if ok {
		delete(lc.sentReqSeqs, reqSeq)
	}
	lc.reqSeqMu.Unlock()
	return ok
}

func (lc *LineClient) PreHandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	key := msg.Content.RelatesTo.GetAnnotationKey()
	ref, err := lc.resolveMatrixReaction(ctx, msg)
	if err != nil {
		return bridgev2.MatrixReactionPreResponse{}, err
	}
	return bridgev2.MatrixReactionPreResponse{
		SenderID:     makeUserID(string(lc.UserLogin.ID)),
		EmojiID:      ref.networkEmojiID(),
		Emoji:        key,
		MaxReactions: 1,
	}, nil
}

func (lc *LineClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	key := msg.Content.RelatesTo.GetAnnotationKey()
	ref, err := lc.resolveMatrixReaction(ctx, msg)
	if err != nil {
		return nil, err
	}
	targetID, err := parseReactionTargetMessageID(msg.TargetMessage.ID)
	if err != nil {
		return nil, err
	}

	reqSeq := lc.nextReqSeq()
	_, err = lc.callLine(ctx, func(client *line.Client) error {
		return client.React(int64(reqSeq), targetID, ref.reactionType())
	})
	if err != nil {
		if line.IsInvalidPaidReactionType(err) {
			return nil, unsupportedMatrixReactionError(key)
		}
		if line.IsNotAMemberError(err) {
			return nil, reactionNotAMemberError()
		}
		return nil, err
	}

	return &database.Reaction{
		EmojiID:  ref.networkEmojiID(),
		Emoji:    key,
		Metadata: ref.metadata(key),
	}, nil
}

func (lc *LineClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	if msg.TargetReaction == nil {
		return errors.New("target reaction is missing")
	}
	targetID, err := parseReactionTargetMessageID(msg.TargetReaction.MessageID)
	if err != nil {
		return err
	}
	reqSeq := lc.nextReqSeq()
	_, err = lc.callLine(ctx, func(client *line.Client) error {
		return client.CancelReaction(int64(reqSeq), targetID)
	})
	if line.IsNotAMemberError(err) {
		return reactionNotAMemberError()
	}
	return err
}
