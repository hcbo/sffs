package com.hcb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SffsFileSystem extends FileSystem {

    public static int BUFFERSIZE;

    public static final Logger LOG = LoggerFactory.getLogger(SffsFileSystem.class);

    private URI uri;
    private Path workingDirectory = new Path("/");

    SSDBUnderFileSystem SSDBUnderFileSystem;

    public URI getUri() {
        LOG.error("hcb"+"getUri"+uri);
        return uri;
    }

    public String getScheme() {
        return "hcbMfs";
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException { // get
        super.initialize(uri, conf);
        PropertyConfigurator.configure(getLog4jProps());
        LOG.error("hcb "+"initialize方法"+uri+conf);
        if (conf.get("bufferSize") == null || conf.get("bufferSize") == "") {
            BUFFERSIZE = PropertyUtils.getBufferSize();
        } else {
            BUFFERSIZE = Integer.parseInt(conf.get("bufferSize"));
        }
        this.uri = uri;
        SSDBUnderFileSystem = new SSDBUnderFileSystem(uri, conf);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        LOG.error("open path: {} bufferSize:{}", path, bufferSize);

        InputStream inputStream = SSDBUnderFileSystem.open(path.toString());
        return new FSDataInputStream(inputStream);

    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, final boolean overwrite, final int bufferSize,
                                     final short replication, final long blockSize, final Progressable progress) throws IOException {

        LOG.error("create path: {} bufferSize:{} blockSize:{}", path, bufferSize, blockSize);
        short mode = permission.toShort();
//        OutputStream outputStream = neuUnderFileSystem.create(path.toString());
        OutputStream outputStream = SSDBUnderFileSystem.create(path.toString(), mode);
        return new FSDataOutputStream(outputStream, statistics);

    }

    /**
     * {@inheritDoc}
     * @throws FileNotFoundException if the parent directory is not present -or
     * is not a directory.
     */
    @Override
    public FSDataOutputStream createNonRecursive(Path path,
                                                 FsPermission permission,
                                                 EnumSet<CreateFlag> flags,
                                                 int bufferSize,
                                                 short replication,
                                                 long blockSize,
                                                 Progressable progress) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            // expect this to raise an exception if there is no parent
            if (!getFileStatus(parent).isDirectory()) {
                throw new FileAlreadyExistsException("Not a directory: " + parent);
            }
        }
        return create(path, permission,
                flags.contains(CreateFlag.OVERWRITE), bufferSize,
                replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable) throws IOException {

        LOG.error("append path: {} bufferSize:{}", path, bufferSize);

//        path = qualify(path);
//        try {
//            OutputStream outputStream = seaweedFileSystemStore.createFile(path, false, null, bufferSize, "");
//            return new FSDataOutputStream(outputStream, statistics);
//        } catch (Exception ex) {
//            LOG.warn("append path: {} bufferSize:{}", path, bufferSize, ex);
//            return null;
//        }
        return null;
    }

    @Override
    public boolean rename(Path src, Path dst) {

        LOG.error("rename path: {} => {}", src, dst);

        return SSDBUnderFileSystem.renameFile(src.toString(),dst.toString());


//        if (src.isRoot()) {
//            return false;
//        }
//
//        if (src.equals(dst)) {
//            return true;
//        }
//        FileStatus dstFileStatus = getFileStatus(dst);
//
//        String sourceFileName = src.getName();
//        Path adjustedDst = dst;
//
//        if (dstFileStatus != null) {
//            if (!dstFileStatus.isDirectory()) {
//                return false;
//            }
//            adjustedDst = new Path(dst, sourceFileName);
//        }
//
//        Path qualifiedSrcPath = qualify(src);
//        Path qualifiedDstPath = qualify(adjustedDst);
//
//        seaweedFileSystemStore.rename(qualifiedSrcPath, qualifiedDstPath);
    }

    @Override
    public boolean delete(Path path, boolean recursive) {


        return SSDBUnderFileSystem.delete(path.toString());
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {

        LOG.error("listStatus path: {}", path);

        return  SSDBUnderFileSystem.listStatus(path.toString());
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    @Override
    public void setWorkingDirectory(Path path) {
        if (path.isAbsolute()) {
            workingDirectory = path;
        } else {
            workingDirectory = new Path(workingDirectory, path);
        }
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {

        LOG.error("SeaWeed.mkdirs path: {}", path);

        return SSDBUnderFileSystem.mkdirs(path.toString());
    }

    @Override
    public FileStatus getFileStatus(Path path) throws FileNotFoundException,
            IOException{

        LOG.error("SeaWeed.getFileStatus path: {}", path);

        return SSDBUnderFileSystem.getFileStatus(path.toString());
    }

    /**
     * Set owner of a path (i.e. a file or a directory).
     * The parameters owner and group cannot both be null.
     *
     * @param path  The path
     * @param owner If it is null, the original username remains unchanged.
     * @param group If it is null, the original groupname remains unchanged.
     */
    @Override
    public void setOwner(Path path, final String owner, final String group)
            throws IOException {
        LOG.error("setOwner path: {}", path);
//        path = qualify(path);
//
//        seaweedFileSystemStore.setOwner(path, owner, group);
    }


    /**
     * Set permission of a path.
     *
     * @param path       The path
     * @param permission Access permission
     */
    @Override
    public void setPermission(Path path, final FsPermission permission) throws IOException {
        LOG.error("setPermission path: {}", path);

        if (permission == null) {
            throw new IllegalArgumentException("The permission can't be null");
        }

        path = qualify(path);

//        seaweedFileSystemStore.setPermission(path, permission);
    }

    Path qualify(Path path) {
        return path.makeQualified(uri, workingDirectory);
    }

    /**
     * Concat existing files together.
     *
     * @param trg   the path to the target destination.
     * @param psrcs the paths to the sources to use for the concatenation.
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default).
     */
    @Override
    public void concat(final Path trg, final Path[] psrcs) throws IOException {
        throw new UnsupportedOperationException("Not implemented by the " +
                getClass().getSimpleName() + " FileSystem implementation");
    }

    /**
     * Truncate the file in the indicated path to the indicated size.
     * <ul>
     * <li>Fails if path is a directory.</li>
     * <li>Fails if path does not exist.</li>
     * <li>Fails if path is not closed.</li>
     * <li>Fails if new size is greater than current size.</li>
     * </ul>
     *
     * @param f         The path to the file to be truncated
     * @param newLength The size the file is to be truncated to
     * @return <code>true</code> if the file has been truncated to the desired
     * <code>newLength</code> and is immediately available to be reused for
     * write operations such as <code>append</code>, or
     * <code>false</code> if a background process of adjusting the length of
     * the last block has been started, and clients should wait for it to
     * complete before proceeding with further file updates.
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default).
     */
    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        throw new UnsupportedOperationException("Not implemented by the " +
                getClass().getSimpleName() + " FileSystem implementation");
    }

    @Override
    public void createSymlink(final Path target, final Path link,
                              final boolean createParent) throws AccessControlException,
            FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, UnsupportedFileSystemException,
            IOException {
        // Supporting filesystems should override this method
        throw new UnsupportedOperationException(
                "Filesystem does not support symlinks!");
    }

    public boolean supportsSymlinks() {
        return false;
    }

    /**
     * Create a snapshot.
     *
     * @param path         The directory where snapshots will be taken.
     * @param snapshotName The name of the snapshot
     * @return the snapshot path.
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     */
    @Override
    public Path createSnapshot(Path path, String snapshotName)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support createSnapshot");
    }

    /**
     * Rename a snapshot.
     *
     * @param path            The directory path where the snapshot was taken
     * @param snapshotOldName Old name of the snapshot
     * @param snapshotNewName New name of the snapshot
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void renameSnapshot(Path path, String snapshotOldName,
                               String snapshotNewName) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support renameSnapshot");
    }

    /**
     * Delete a snapshot of a directory.
     *
     * @param path         The directory that the to-be-deleted snapshot belongs to
     * @param snapshotName The name of the snapshot
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void deleteSnapshot(Path path, String snapshotName)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support deleteSnapshot");
    }

    /**
     * Modifies ACL entries of files and directories.  This method can add new ACL
     * entries or modify the permissions on existing ACL entries.  All existing
     * ACL entries that are not specified in this call are retained without
     * changes.  (Modifications are merged into the current ACL.)
     *
     * @param path    Path to modify
     * @param aclSpec List&lt;AclEntry&gt; describing modifications
     * @throws IOException                   if an ACL could not be modified
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support modifyAclEntries");
    }

    /**
     * Removes ACL entries from files and directories.  Other ACL entries are
     * retained.
     *
     * @param path    Path to modify
     * @param aclSpec List describing entries to remove
     * @throws IOException                   if an ACL could not be modified
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support removeAclEntries");
    }

    /**
     * Removes all default ACL entries from files and directories.
     *
     * @param path Path to modify
     * @throws IOException                   if an ACL could not be modified
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void removeDefaultAcl(Path path)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support removeDefaultAcl");
    }

    /**
     * Removes all but the base ACL entries of files and directories.  The entries
     * for user, group, and others are retained for compatibility with permission
     * bits.
     *
     * @param path Path to modify
     * @throws IOException                   if an ACL could not be removed
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void removeAcl(Path path)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support removeAcl");
    }

    /**
     * Fully replaces ACL of files and directories, discarding all existing
     * entries.
     *
     * @param path    Path to modify
     * @param aclSpec List describing modifications, which must include entries
     *                for user, group, and others for compatibility with permission bits.
     * @throws IOException                   if an ACL could not be modified
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support setAcl");
    }

    /**
     * Gets the ACL of a file or directory.
     *
     * @param path Path to get
     * @return AclStatus describing the ACL of the file or directory
     * @throws IOException                   if an ACL could not be read
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support getAclStatus");
    }

    /**
     * Set an xattr of a file or directory.
     * The name must be prefixed with the namespace followed by ".". For example,
     * "user.attr".
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path  Path to modify
     * @param name  xattr name.
     * @param value xattr value.
     * @param flag  xattr set flag
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void setXAttr(Path path, String name, byte[] value,
                         EnumSet<XAttrSetFlag> flag) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support setXAttr");
    }

    /**
     * Get an xattr name and value for a file or directory.
     * The name must be prefixed with the namespace followed by ".". For example,
     * "user.attr".
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path Path to get extended attribute
     * @param name xattr name.
     * @return byte[] xattr value.
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support getXAttr");
    }

    /**
     * Get all of the xattr name/value pairs for a file or directory.
     * Only those xattrs which the logged-in user has permissions to view
     * are returned.
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path Path to get extended attributes
     * @return Map describing the XAttrs of the file or directory
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support getXAttrs");
    }

    /**
     * Get all of the xattrs name/value pairs for a file or directory.
     * Only those xattrs which the logged-in user has permissions to view
     * are returned.
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path  Path to get extended attributes
     * @param names XAttr names.
     * @return Map describing the XAttrs of the file or directory
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support getXAttrs");
    }

    /**
     * Get all of the xattr names for a file or directory.
     * Only those xattr names which the logged-in user has permissions to view
     * are returned.
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path Path to get extended attributes
     * @return List{@literal <String>} of the XAttr names of the file or directory
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public List<String> listXAttrs(Path path) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support listXAttrs");
    }

    /**
     * Remove an xattr of a file or directory.
     * The name must be prefixed with the namespace followed by ".". For example,
     * "user.attr".
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path Path to remove extended attribute
     * @param name xattr name
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void removeXAttr(Path path, String name) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support removeXAttr");
    }


    private Properties getLog4jProps(){
        Properties props = new Properties();
        String logPath = PropertyUtils.getLogPath();
        props.put("log4j.appender.FileAppender","org.apache.log4j.RollingFileAppender");
        props.put("log4j.appender.FileAppender.File",logPath);
        props.put("log4j.appender.FileAppender.layout","org.apache.log4j.PatternLayout");
        props.put("log4j.appender.FileAppender.layout.ConversionPattern","%-4r [%t] %-5p %c %x - %m%n");
        props.put("log4j.rootLogger","ERROR, FileAppender");
        return props;
    }

}
