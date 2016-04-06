/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterableHandle;
import jetbrains.exodus.entitystore.EntityIterableType;
import jetbrains.exodus.entitystore.PersistentEntityStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.UnsupportedEncodingException;

@SuppressWarnings({"AbstractClassWithoutAbstractMethods", "RawUseOfParameterizedType", "ProtectedField"})
public abstract class EntityIterableHandleBase implements EntityIterableHandle {

    private static final int HASH_LONGS_COUNT = 4; // NB: the fact that it is a power of 2 is used

    @Nullable
    private final PersistentEntityStore store;
    @NotNull
    private final EntityIterableType type;
    @Nullable
    private EntityIterableHandleHash hash;
    @NotNull
    private LinksFilter linksFilter;

    protected EntityIterableHandleBase(@Nullable final PersistentEntityStore store,
                                       @NotNull final EntityIterableType type) {
        this.store = store;
        this.type = type;
        hash = null;
        linksFilter = new InitialLinksFilter();
    }

    @Override
    @NotNull
    public EntityIterableType getType() {
        return type;
    }

    @Override
    public final boolean hasLinkId(int id) {
        return linksFilter.hasLinkId(id);
    }

    @Nullable
    public PersistentEntityStore getStore() {
        return store;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object obj) {
        if (!(obj instanceof EntityIterableHandle)) {
            return false;
        }
        final EntityIterableHandle that = (EntityIterableHandle) obj;
        return getIdentity().equals(that.getIdentity());
    }

    public final int hashCode() {
        return getIdentity().hashCode();
    }

    @NotNull
    public final Object getIdentity() {
        if (hash == null) {
            hash = new EntityIterableHandleHash(store);
            hash.apply(type.getType());
            if (type != EntityIterableType.EMPTY) {
                hash.applyDelimiter();
            }
            hashCode(hash);
            hash.computeHashCode();
        }
        return hash;
    }

    @Override
    public boolean isMatchedEntityAdded(@NotNull final EntityId added) {
        return true;
    }

    @Override
    public boolean isMatchedEntityDeleted(@NotNull final EntityId deleted) {
        return true;
    }

    @Override
    public boolean isMatchedPropertyChanged(final int typeId,
                                            final int propertyId,
                                            @Nullable final Comparable oldValue,
                                            @Nullable final Comparable newValue) {
        return true;
    }

    @NotNull
    @Override
    public int[] getLinkIds() {
        return LinksFilter.EMPTY_LINKS_ARRAY;
    }

    @Override
    public boolean isExpired() {
        return false;
    }

    @Override
    public final String toString() {
        final StringBuilder builder = new StringBuilder();
        toString(builder);
        return builder.toString();
    }

    public void toString(@NotNull final StringBuilder builder) {
        builder.append(type.getType());
        if (type != EntityIterableType.EMPTY) {
            builder.append('-');
        }
    }

    public abstract void hashCode(@NotNull final EntityIterableHandleHash hash);

    @NotNull
    public static int[] mergeLinkIds(@NotNull final int[] left, @NotNull final int[] right) {
        final int l = left.length;
        if (l == 0) return right;
        final int r = right.length;
        if (r == 0) return left;
        final int mergedLength = getMergedLength(left, right, l, r);
        if (mergedLength == l) return left;
        if (mergedLength == r) return right;
        return merge(left, right, l, r, new int[mergedLength]);
    }

    private static int getMergedLength(int[] left, int[] right, int l, int r) {
        int i = 0, j = 0, k = 0;
        while (true) {
            final int a = left[i];
            final int b = right[j];
            if (a <= b) {
                i++;
                k++;
                if (i >= l) {
                    if (a == b) {
                        j++;
                    }
                    k += r - j;
                    break;
                }
                if (a == b) {
                    j++;
                } else {
                    continue;
                }
            } else {
                j++;
                k++;
            }
            if (j >= r) {
                k += l - i;
                break;
            }
        }
        return k;
    }

    private static int[] merge(int[] left, int[] right, int l, int r, int[] result) {
        int i = 0, j = 0, k = 0;
        int a = left[0];
        int b = right[0];
        while (true) {
            if (a <= b) {
                i++;
                result[k++] = a;
                if (i < l) {
                    boolean neq = a != b;
                    a = left[i];
                    if (neq) {
                        continue;
                    }
                    j++;
                } else {
                    if (a == b) {
                        j++;
                    }
                    while (j < r) {
                        result[k++] = right[j++];
                    }
                    break;
                }
            } else {
                j++;
                result[k++] = b;
            }
            if (j < r) {
                b = right[j];
            } else {
                while (i < l) {
                    result[k++] = left[i++];
                }
                break;
            }
        }
        return result;
    }

    public static final class EntityIterableHandleHash {

        private static final String UTF_8 = "UTF-8";
        private static final byte[][] INTS;

        static {
            INTS = new byte[1024][];
            try {
                for (int i = 0; i < INTS.length; ++i) {
                    INTS[i] = Integer.toString(i).getBytes(UTF_8);
                }
            } catch (UnsupportedEncodingException e) {
                throw ExodusException.toExodusException(e);
            }
        }

        @NotNull
        private final long[] hashLongs;
        private int bytesProcessed;
        private int hashCode;

        public EntityIterableHandleHash(@Nullable final PersistentEntityStore store) {
            hashLongs = new long[HASH_LONGS_COUNT];
            hashCode = store == null ? 0 : System.identityHashCode(store);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof EntityIterableHandleHash)) {
                return false;
            }
            final EntityIterableHandleHash rightHash = (EntityIterableHandleHash) obj;
            if (hashCode != rightHash.hashCode) {
                return false;
            }
            final long[] rightHashLongs = rightHash.hashLongs;
            for (int i = 0; i < hashLongs.length; ++i) {
                if (hashLongs[i] != rightHashLongs[i]) {
                    return false;
                }
            }
            return true;
        }

        public void apply(final byte b) {
            final int bytesProcessed = this.bytesProcessed;
            if (bytesProcessed < HASH_LONGS_COUNT * 8) {
                hashLongs[((bytesProcessed >> 3) & (HASH_LONGS_COUNT - 1))] += (((long) (b & 0xff)) << ((bytesProcessed & 7) << 3));
            } else {
                final int index = bytesProcessed & (HASH_LONGS_COUNT - 1);
                final long hashValue = hashLongs[index];
                hashLongs[index] = (hashValue << 5) - hashValue /* same as hashValue*31 */ + (b & 0xff);
            }
            this.bytesProcessed = bytesProcessed + 1;
        }

        public void apply(final byte[] bytes) {
            for (byte b : bytes) {
                apply(b);
            }
        }

        public void apply(final int i) {
            if (i >= 0 && i < INTS.length) {
                apply(INTS[i]);
            } else {
                apply(Integer.toString(i));
            }
        }

        public void apply(final long l) {
            if (l >= 0 && l < (long) INTS.length) {
                apply(INTS[((int) l)]);
            } else {
                apply(Long.toString(l));
            }
        }

        public void apply(@NotNull final String s) {
            try {
                for (final byte b : s.getBytes(UTF_8)) {
                    apply(b);
                }
            } catch (UnsupportedEncodingException e) {
                throw ExodusException.toExodusException(e);
            }
        }

        public void apply(@NotNull final EntityIterableHandle source) {
            ((EntityIterableHandleHash) source.getIdentity()).forEachByte(new ByteConsumer() {
                @Override
                public void accept(final byte b) {
                    apply(b);
                }
            });
        }

        public void applyDelimiter() {
            apply((byte) '-');
        }

        @Override
        public String toString() {
            final int hashBytes = Math.min(bytesProcessed, HASH_LONGS_COUNT * 8);
            final StringBuilder builder = new StringBuilder(hashBytes);
            forEachByte(new ByteConsumer() {
                @Override
                public void accept(final byte b) {
                    builder.append((char) b);
                }
            });
            return builder.toString();
        }

        // method is public for tests only
        public void computeHashCode() {
            long result = 314159265358L;
            for (final long hl : hashLongs) {
                result += hl;
            }
            hashCode += (int) result;
            hashCode += (int) (result >>> 32);
        }

        private void forEachByte(@NotNull final ByteConsumer consumer) {
            final int hashBytes = Math.min(bytesProcessed, HASH_LONGS_COUNT * 8);
            long hashLong = 0;
            for (int i = 0; i < hashBytes; i++) {
                if ((i & 7) == 0) {
                    hashLong = hashLongs[i >> 3];
                }
                consumer.accept((byte) (hashLong & 0xff));
                hashLong >>= 8;
            }
        }

        private interface ByteConsumer {

            void accept(final byte b);
        }
    }

    interface LinksFilter {

        int[] EMPTY_LINKS_ARRAY = new int[0];

        boolean hasLinkId(int linkId);
    }

    private static class TrivialLinksFilter implements LinksFilter {

        public static final LinksFilter INSTANCE = new TrivialLinksFilter();

        @Override
        public boolean hasLinkId(int linkId) {
            return false;
        }
    }

    private class InitialLinksFilter implements LinksFilter {

        @Override
        public boolean hasLinkId(int linkId) {
            final int[] linkIds = getLinkIds();
            final int linksCount = linkIds.length;
            if (linksCount == 0) {
                linksFilter = TrivialLinksFilter.INSTANCE;
                return false;
            }
            if (linksCount == 1) {
                final int singleId = linkIds[0];
                linksFilter = new SingleLinkFilter(singleId);
                return singleId == linkId;
            }
            linksFilter = linksCount < 4 ? new LinearSearchLinksFilter(linkIds) : new BinarySearchLinksFilter(linkIds);
            return linksFilter.hasLinkId(linkId);
        }
    }

    private static class SingleLinkFilter implements LinksFilter {

        private final int linkId;

        private SingleLinkFilter(final int linkId) {
            this.linkId = linkId;
        }

        @Override
        public boolean hasLinkId(int linkId) {
            return linkId == this.linkId;
        }
    }

    private static abstract class BloomLinksFilter implements LinksFilter {

        protected final int[] ids;
        private final int bloomFilter;

        private BloomLinksFilter(@NotNull final int[] ids) {
            this.ids = ids;
            int bloomFilter = 0;
            for (int id : ids) {
                bloomFilter |= (1 << (id & 0x1f));
            }
            this.bloomFilter = bloomFilter;
        }

        @Override
        public boolean hasLinkId(int linkId) {
            return (bloomFilter & (1 << (linkId & 0x1f))) != 0;
        }
    }

    private static class LinearSearchLinksFilter extends BloomLinksFilter {

        private LinearSearchLinksFilter(@NotNull final int[] ids) {
            super(ids);
        }

        @Override
        public boolean hasLinkId(int linkId) {
            if (super.hasLinkId(linkId)) {
                for (int id : ids) {
                    if (id == linkId) {
                        return true;
                    }
                    if (id > linkId) {
                        break;
                    }
                }
            }
            return false;
        }
    }

    private static class BinarySearchLinksFilter extends BloomLinksFilter {

        private BinarySearchLinksFilter(@NotNull final int[] ids) {
            super(ids);
        }

        @Override
        public boolean hasLinkId(int linkId) {
            if (super.hasLinkId(linkId)) {
                // copy-pasted Arrays.binarySearch
                int high = ids.length - 1;
                int low = 0;
                while (low <= high) {
                    final int mid = (low + high) >>> 1;
                    final int midVal = ids[mid];
                    if (midVal < linkId) {
                        low = mid + 1;
                    } else if (midVal > linkId) {
                        high = mid - 1;
                    } else {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
